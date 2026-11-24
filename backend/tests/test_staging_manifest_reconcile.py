"""集成测试：暂存分片可确认落地清单与到达对账的版本化消费、中断恢复和余量另开对账。

场景主线：
1. 同一批次重跑落地产生两代暂存数据，旧代完整可读、新代登记计划条数但缺最后一片；
2. 末片暂停期间提交对账：对账只能消费 confirmed 分片，挂起待清单确认且无业务回执；
3. 重启落地进程补齐末片，挂起对账自动续做并形成回执，按实际分片明细复算条数；
4. 恢复入口能辨明已完成分区，工作台可见旧分区结算结果与挂起请求的消费范围；
5. 按实际分区明细规划尚未覆盖的余量对账。
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.services.base import DATA_ROOT
from app.services.transfer_batch import TransferBatchService
from app.services.staging_area import StagingAreaService
from app.services.arrival_reconcile import ArrivalReconcileService
from app.store import store


@pytest.fixture(autouse=True)
def reset_store():
    """每个用例重置内存表，批次编号带用例后缀避免文件产物相互干扰。"""
    from app.seed import SEED_ROWS
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    yield


def _prepare_batch(batch_code: str, record_count: int, chunk_size: int = 5):
    """构造传输批次产物文件，并以调用方给定编号复制一份，供暂存按固定批次号读取。"""
    tb = TransferBatchService()
    batch = tb.create_batch(f"EXTR-{batch_code}", record_count, "TARG-0001")
    tb.start_transfer(batch["batch_id"], chunk_size=chunk_size)
    real_code = batch["batch_code"]
    source = DATA_ROOT / "batches" / f"{real_code}.jsonl"
    target = DATA_ROOT / "batches" / f"{batch_code}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    return batch_code, record_count


class TestTwoGenerationsAndManifest:
    """两代暂存并存与可确认落地清单。"""

    def test_rerun_landing_keeps_old_generation_readable_and_new_missing_last_shard(self):
        batch_code, total = _prepare_batch("BATCH-TWO-GEN", 25)  # shard_size=10 -> 3 片
        sa = StagingAreaService()

        # 第一代：完整落地并确认
        gen1 = sa.land_batch(batch_code, total, shard_size=10)
        assert gen1["generation"] == 1
        assert gen1["manifest_status"] == "confirmed"
        assert gen1["planned_shard_count"] == 3
        sa.confirm_land(
            next(r["id"] for r in store.rows("staging_area")
                 if r["批次编号"] == batch_code and r["代际"] == "1"))

        # 第二代：暂停最后一片，清单停留 partial
        gen2 = sa.land_batch(batch_code, total, shard_size=10, pause_before_last_shard=True)
        assert gen2["generation"] == 2
        assert gen2["paused"] is True
        assert gen2["confirmed_shards"] == 2
        assert gen2["planned_shard_count"] == 3
        assert gen2["manifest_status"] == "partial"

        # 两代目录相互独立，旧代产物仍完整可读
        old_dir = DATA_ROOT / "staging" / batch_code / "gen1"
        new_dir = DATA_ROOT / "staging" / batch_code / "gen2"
        assert (old_dir / "manifest.json").exists()
        assert (old_dir / "shard_0000.jsonl").exists()
        assert (old_dir / "shard_0002.jsonl").exists()
        assert (new_dir / "shard_0000.jsonl").exists()
        assert not (new_dir / "shard_0002.jsonl").exists()

        # 新代清单登记了计划条数与全部规划分片，末片为 planned
        manifest2 = sa.get_manifest(batch_code, 2)
        assert manifest2["planned_total_records"] == total
        assert [s["shard_index"] for s in manifest2["shards"]] == [0, 1, 2]
        assert [s["status"] for s in manifest2["shards"]] == ["confirmed", "confirmed", "planned"]

        # 旧代清单仍可完整读取
        old_manifest = sa.latest_confirmed_manifest(batch_code)
        assert old_manifest["generation"] == 1
        assert sa._manifest_readable(old_manifest) is True
        # partial 清单不可被当作完整清单消费
        assert sa._manifest_readable(manifest2) is False

    def test_concurrent_reconcile_while_last_shard_missing_settles_on_old_generation(self):
        batch_code, total = _prepare_batch("BATCH-CONCURRENT", 20)  # 2 片
        sa = StagingAreaService()
        ar = ArrivalReconcileService()

        sa.land_batch(batch_code, total, shard_size=10)
        sa.land_batch(batch_code, total, shard_size=10, pause_before_last_shard=True)

        # 显式按旧分区（代际1）提交对账，应完整平账
        recon_old = ar.create_reconciliation(batch_code, total, generation=1)
        result_old = ar.execute_reconciliation(recon_old["recon_id"])
        assert result_old["balanced"] is True
        assert result_old["actual"] == total
        assert result_old["generation"] == 1
        assert result_old["consumed_shard_indexes"] == [0, 1]
        # 条数必须与同一版本实际分片数据对应
        per_shard = result_old["per_shard"]
        assert all(p["matches_manifest"] for p in per_shard)
        assert sum(p["actual_record_count"] for p in per_shard) == total

        # 默认绑定最新代际（代际2）的对账因末片缺失而挂起，不产生回执
        recon_new = ar.create_reconciliation(batch_code, total)
        assert recon_new["generation"] == 2
        result_new = ar.execute_reconciliation(recon_new["recon_id"])
        assert result_new["status"] == "待清单确认"
        assert result_new["receipt_code"] is None
        assert result_new["planned_shard_indexes"] == [0, 1]
        assert result_new["consumable_shard_indexes"] == []
        entry = ar.get_entry(recon_new["recon_id"])
        assert entry["status"] == "待清单确认"
        assert entry["回执编号"] == ""
        # 恢复期间不存在该请求的业务回执
        assert ar.get_receipt(recon_new["recon_id"]) is None
        # 旧分区对账回执已经形成
        assert ar.get_receipt(recon_old["recon_id"])["balanced"] is True


class TestReconcileConsumesOnlyConfirmedShards:
    """对账只读取可确认落地清单中的完整分片。"""

    def test_reconcile_actual_count_recomputed_from_shard_details(self):
        batch_code, total = _prepare_batch("BATCH-RECOMPUTE", 23)  # 3 片: 10/10/3
        sa = StagingAreaService()
        ar = ArrivalReconcileService()

        landing = sa.land_batch(batch_code, total, shard_size=10, pause_before_last_shard=True)
        assert landing["confirmed_shards"] == 2

        # 挂起对账（绑定 partial 新清单）
        recon = ar.create_reconciliation(batch_code, total)
        waiting = ar.execute_reconciliation(recon["recon_id"])
        assert waiting["status"] == "待清单确认"

        # 恢复落地：补齐末片
        resumed = sa.resume_landing(batch_code)
        assert resumed["resumed_shards"] == [2]
        assert resumed["manifest_status"] == "confirmed"
        assert resumed["records_landed"] == total

        # 自动续做对账，按分片明细复算
        auto = ar.resume_pending_reconciliations(batch_code)
        assert len(auto["resumed"]) == 1
        settled = auto["resumed"][0]
        assert settled["balanced"] is True
        assert settled["actual"] == total
        assert settled["consumed_shard_indexes"] == [0, 1, 2]
        counts = sorted((p["shard_index"], p["actual_record_count"]) for p in settled["per_shard"])
        assert counts == [(0, 10), (1, 10), (2, 3)]

        # 续做后形成业务回执，恢复期间（挂起阶段）没有任何回执文件
        receipt = ar.get_receipt(recon["recon_id"])
        assert receipt is not None
        assert receipt["trigger"] == "resume"
        assert receipt["actual"] == total
        assert receipt["consumed_shard_indexes"] == [0, 1, 2]
        receipt_file = DATA_ROOT / "receipts" / f"{receipt['receipt_code']}.json"
        assert receipt_file.exists()

    def test_no_receipt_issued_during_recovery_window(self):
        batch_code, total = _prepare_batch("BATCH-NO-RECEIPT", 10)
        sa = StagingAreaService()
        ar = ArrivalReconcileService()
        sa.land_batch(batch_code, total, shard_size=10, pause_before_last_shard=True)

        recon = ar.create_reconciliation(batch_code, total)
        ar.execute_reconciliation(recon["recon_id"])
        receipts_before = list((DATA_ROOT / "receipts").glob("*.json")) if (DATA_ROOT / "receipts").exists() else []
        assert not any(recon["recon_code"] in p.name for p in receipts_before)

        # 重复调用恢复入口但清单仍缺片：依旧无回执、保持挂起
        again = ar.resume_pending_reconciliations(batch_code)
        assert again["resumed"] == []
        assert len(again["still_waiting"]) == 1
        assert ar.get_receipt(recon["recon_id"]) is None

        # 补齐后续做才发回执
        sa.resume_landing(batch_code)
        ar.resume_pending_reconciliations(batch_code)
        receipt = ar.get_receipt(recon["recon_id"])
        assert receipt is not None and receipt["balanced"] is True


class TestRecoveryEntryAndRemainderReconciliation:
    """恢复入口辨明分区身份、工作台视图与余量另开对账。"""

    def test_recovery_view_identifies_completed_and_interrupted_partitions(self):
        batch_code, total = _prepare_batch("BATCH-RECOVERY-VIEW", 20)
        sa = StagingAreaService()
        ar = ArrivalReconcileService()

        sa.land_batch(batch_code, total, shard_size=10)
        gen1_recon = ar.create_reconciliation(batch_code, total, generation=1)
        ar.execute_reconciliation(gen1_recon["recon_id"])  # 旧分区已结算

        sa.land_batch(batch_code, total, shard_size=10, pause_before_last_shard=True)
        pending_recon = ar.create_reconciliation(batch_code, total)
        ar.execute_reconciliation(pending_recon["recon_id"])  # 新分区挂起

        view = sa.recovery_view(batch_code)
        partitions = {p["generation"]: p for p in view["partitions"]}
        assert partitions[1]["readable"] is True
        assert partitions[1]["manifest_status"] == "confirmed"
        assert partitions[1]["confirmed_shards"] == 2
        assert partitions[2]["readable"] is False
        assert partitions[2]["manifest_status"] == "partial"
        assert partitions[2]["confirmed_shards"] == 1
        assert partitions[2]["confirmed_shard_indexes"] == [0]

        # 工作台视角：旧分区结算结果
        old_entry = ar.get_entry(gen1_recon["recon_id"])
        assert old_entry["status"] == "已平账"
        assert old_entry["回执编号"] != ""
        # 挂起请求及其消费范围可见
        pending_list = ar.get_pending_reconciliations(batch_code)
        assert len(pending_list) == 1
        assert pending_list[0]["recon_id"] == pending_recon["recon_id"]
        assert pending_list[0]["generation"] == 2
        assert pending_list[0]["planned_shard_indexes"] == [0, 1]
        assert pending_list[0]["consumable_shard_indexes"] == []

    def test_remainder_reconciliation_after_last_shard_completed(self):
        batch_code, total = _prepare_batch("BATCH-REMAINDER", 25)  # 3 片: 10/10/5
        sa = StagingAreaService()
        ar = ArrivalReconcileService()

        # 第一代完整，第二代落地中断在末片
        sa.land_batch(batch_code, total, shard_size=10)
        sa.land_batch(batch_code, total, shard_size=10, pause_before_last_shard=True)

        # 中断期间按第二代已确认分片（前两片，实到 20 条）做一次部分范围对账
        partial_manifest = sa.get_manifest(batch_code, 2)
        confirmed_indexes = [s["shard_index"] for s in partial_manifest["shards"]
                             if s["status"] == "confirmed"]
        partial_records = sum(
            s["record_count"] for s in partial_manifest["shards"]
            if s["shard_index"] in confirmed_indexes)
        early_recon = ar.create_reconciliation(
            batch_code, partial_records, generation=2, shard_scope=confirmed_indexes)
        waiting = ar.execute_reconciliation(early_recon["recon_id"])
        assert waiting["status"] == "待清单确认"
        # 挂起请求的消费范围可见：规划两片可读、末片待落地
        assert waiting["planned_shard_indexes"] == [0, 1, 2]

        # 重启落地补齐末片，挂起对账自动续做，只结算其登记的前两片范围
        sa.resume_landing(batch_code)
        resumed = ar.resume_pending_reconciliations(batch_code)
        early_settled = resumed["resumed"][0]
        assert early_settled["consumed_shard_indexes"] == [0, 1]
        assert early_settled["actual"] == 20
        assert early_settled["balanced"] is True

        # 按实际分区明细复算：末片（分片2，5 条）尚未被任何对账覆盖，需另开对账
        plan = ar.plan_remainder_reconciliations(batch_code)
        gen2_remainder = [p for p in plan["remainder_plans"] if p["generation"] == 2]
        assert len(gen2_remainder) == 1
        assert gen2_remainder[0]["uncovered_shard_indexes"] == [2]
        assert gen2_remainder[0]["expected_count"] == 5

        # 另开的余量对账执行后按末片实际明细平账
        follow_up = ar.execute_reconciliation(gen2_remainder[0]["recon_id"])
        assert follow_up["balanced"] is True
        assert follow_up["actual"] == 5
        assert follow_up["consumed_shard_indexes"] == [2]
        assert ar.get_receipt(gen2_remainder[0]["recon_id"]) is not None

        # 第二代分片均已被完整覆盖后，再次规划不应为第二代产生新余量对账
        # （第一代在本用例未发起过对账，按规则仍会整体列为余量，属预期）
        second_plan = ar.plan_remainder_reconciliations(batch_code)
        assert [p for p in second_plan["remainder_plans"] if p["generation"] == 2] == []
        assert [p for p in second_plan["remainder_plans"] if p["generation"] == 1][0][
            "uncovered_shard_indexes"] == [0, 1, 2]

    def test_resume_landing_is_idempotent_on_confirmed_shards(self):
        batch_code, total = _prepare_batch("BATCH-IDEMPOTENT", 15)
        sa = StagingAreaService()
        landing = sa.land_batch(batch_code, total, shard_size=10, pause_before_last_shard=True)
        first_resume = sa.resume_landing(batch_code, generation=landing["generation"])
        assert first_resume["resumed_shards"] == [1]
        # 再次恢复：没有未确认分片，不重写已确认分片，清单仍完整
        second_resume = sa.resume_landing(batch_code, generation=landing["generation"])
        assert second_resume["resumed_shards"] == []
        manifest = sa.get_manifest(batch_code, landing["generation"])
        assert manifest["status"] == "confirmed"
        assert all(s["status"] == "confirmed" for s in manifest["shards"])
        # 分片文件内容条数与清单登记一致
        for shard in manifest["shards"]:
            assert len(sa._read_jsonl(Path(shard["artifact_path"]))) == shard["record_count"]

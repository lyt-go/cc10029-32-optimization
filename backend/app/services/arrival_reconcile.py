"""到达对账领域服务：基于可确认落地清单核对批次应到与实到记录数，支撑中断恢复与余量另开对账。

对账请求在创建时绑定一个暂存代际的落地清单：
- 若该清单已全部确认且分片可完整读取，立即按清单中的分片集合结算，实到条数按分片明细
  逐片复算，与同一版本实际数据严格对应；
- 若清单仍是 partial（例如末片暂停），对账进入「待清单确认」并登记消费范围，落地恢复、
  清单补齐后由 resume_pending_reconciliations 自动续做，恢复期间不会产生业务回执；
- 若同批次存在更早的完整可读代际，可显式选择按旧分区结算，两代数据互不串读。
每次结算生成业务回执（receipt），并可按实际分区明细规划尚未被任何对账覆盖的余量对账。
"""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ArrivalReconcileService(BaseModuleService):
    MODULE = "arrival_reconcile"
    ENTITY = "对账单"
    OPERATION_TERM = "校验"
    LIST_FIELDS = [
        "对账编号", "批次编号", "应到条数", "实到条数", "迟到条数", "缺失条数", "对账窗口",
        "对账结论", "清单编号", "暂存代际", "消费分片", "源批信息", "回执编号",
    ]
    REQUIRED_FIELDS = ["对账编号", "批次编号", "应到条数"]
    STATUS_ORDER = ["草稿", "待对账", "对账中", "待清单确认", "已平账", "存在差异"]
    ACTION_RULES = {
        "启动对账": "对账中",
        "等待清单": "待清单确认",
        "确认平账": "已平账",
        "标记差异": "存在差异",
    }
    NEGATIVE_ACTIONS = ["标记差异"]

    def _staging_service(self):
        return self._safe_import("app.services.staging_area", "StagingAreaService")()

    # -- 创建对账请求 --

    def create_reconciliation(
        self,
        batch_code: str,
        expected_count: int,
        generation: int | None = None,
        shard_scope: list[int] | None = None,
    ) -> dict[str, Any]:
        """为传输批次创建到达对账单，登记应到条数并绑定目标暂存代际清单。

        generation 为空时默认绑定同批次代际号最大的分区（含落地中的新分区）；
        传入旧代际号则显式要求按旧分区结算。shard_scope 非空时只消费清单内指定分片，
        用于末片补齐后针对余量分片另开对账。
        """
        sa_svc = self._staging_service()
        manifests = sa_svc.list_manifests(batch_code)
        target_manifest: dict[str, Any] | None = None
        if generation is not None:
            target_manifest = next(
                (m for m in manifests if int(m["generation"]) == int(generation)), None)
        elif manifests:
            target_manifest = manifests[-1]

        recon_code = f"ARRI-{batch_code}-{uuid.uuid4().hex[:10]}"
        values = {
            "对账编号": recon_code,
            "批次编号": batch_code,
            "应到条数": str(expected_count),
            "实到条数": "0",
            "迟到条数": "0",
            "缺失条数": "0",
            "对账窗口": "daily",
            "对账结论": "待对账",
            "清单编号": target_manifest["manifest_code"] if target_manifest else "",
            "暂存代际": str(target_manifest["generation"]) if target_manifest else "",
            "消费分片": "",
            "源批信息": "",
            "回执编号": "",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建对账单失败: {missing}")
        scoped = sorted(int(i) for i in shard_scope) if shard_scope is not None else None
        if scoped is not None:
            # create_entry 仅保留 LIST_FIELDS，分片范围作为附加控制字段补挂在条目上
            entry["分片范围"] = json.dumps(scoped)
        return {"recon_code": recon_code, "recon_id": entry["id"], "batch_code": batch_code,
                "expected_count": expected_count,
                "generation": int(target_manifest["generation"]) if target_manifest else None,
                "manifest_code": target_manifest["manifest_code"] if target_manifest else None,
                "shard_scope": scoped}

    # -- 执行对账 --

    def execute_reconciliation(self, recon_id: int) -> dict[str, Any]:
        """执行到达对账：只读取绑定清单中可完整读取的 confirmed 分片，按分片明细复算条数。"""
        recon_entry = self.get_entry(recon_id)
        if recon_entry is None:
            return {"error": f"对账单 {recon_id} 不存在"}

        # 已结算过的请求不允许重复产生回执，保证恢复期间回执语义唯一
        if recon_entry.get("status") in ("已平账", "存在差异"):
            return {"error": "对账单已结算，请勿重复执行", "recon_id": recon_id,
                    "receipt_code": recon_entry.get("回执编号")}

        entry, msg = self.run_action(recon_id, "启动对账")
        if entry is None:
            return {"error": msg, "recon_id": recon_id}

        batch_code = recon_entry["批次编号"]
        expected = int(recon_entry.get("应到条数", 0))
        bound_generation = int(recon_entry["暂存代际"]) if recon_entry.get("暂存代际") else None
        sa_svc = self._staging_service()

        manifest: dict[str, Any] | None = None
        if bound_generation is not None:
            candidate = sa_svc.get_manifest(batch_code, bound_generation)
            if candidate and candidate.get("status") == "confirmed" and sa_svc._manifest_readable(candidate):
                manifest = candidate

        if manifest is None:
            if bound_generation is not None:
                # 绑定的新分区还缺片（如末片暂停）：登记消费范围后挂起，等清单确认自动续做，
                # 此路径不写任何业务回执
                self.run_action(recon_id, "等待清单")
                planned_shards: list[int] = []
                partial = sa_svc.get_manifest(batch_code, bound_generation)
                if partial is not None:
                    planned_shards = [int(s["shard_index"]) for s in partial["shards"]]
                scope = self._entry_shard_scope(recon_entry)
                consumable = (
                    [i for i in planned_shards if i in set(scope)] if scope is not None else []
                )
                entry["消费分片"] = json.dumps(
                    {"generation": bound_generation, "planned_shard_indexes": planned_shards,
                     "consumable_shard_indexes": consumable,
                     "requested_shard_indexes": scope}, ensure_ascii=False)
                self._write_jsonl(
                    self._data_dir("events") / "reconcile_wait_log.jsonl",
                    [{"action": "wait_manifest", "recon_code": recon_entry["对账编号"],
                      "batch_code": batch_code, "generation": bound_generation,
                      "planned_shard_indexes": planned_shards, "at": time.time()}],
                )
                return {
                    "recon_code": recon_entry["对账编号"],
                    "batch_code": batch_code,
                    "expected": expected,
                    "status": "待清单确认",
                    "generation": bound_generation,
                    "planned_shard_indexes": planned_shards,
                    "consumable_shard_indexes": consumable,
                    "receipt_code": None,
                    "message": "绑定清单尚未确认完整，对账挂起，清单补齐后自动续做",
                }
            # 未绑定任何暂存清单（无落地产物的历史批次）：保留缺数回退结算
            return self._settle_without_manifest(recon_entry, expected)

        return self._settle(recon_entry, manifest, expected, trigger="execute",
                            shard_scope=self._entry_shard_scope(recon_entry))

    @staticmethod
    def _entry_shard_scope(entry: dict[str, Any]) -> list[int] | None:
        """读取对账请求限定的分片范围；未限定返回 None 表示消费清单全部分片。"""
        raw = entry.get("分片范围")
        if not raw:
            return None
        try:
            return sorted(int(i) for i in json.loads(raw))
        except json.JSONDecodeError:
            return None

    def _settle(
        self,
        recon_entry: dict[str, Any],
        manifest: dict[str, Any],
        expected: int,
        trigger: str,
        shard_scope: list[int] | None = None,
    ) -> dict[str, Any]:
        """按清单分片集合完成一次结算：实到条数逐片复算，落业务回执并驱动 SLA/血缘。"""
        sa_svc = self._staging_service()
        batch_code = recon_entry["批次编号"]
        if shard_scope is None:
            target_shards = sorted(manifest["shards"], key=lambda s: int(s["shard_index"]))
        else:
            wanted = set(shard_scope)
            target_shards = [s for s in sorted(manifest["shards"], key=lambda s: int(s["shard_index"]))
                             if int(s["shard_index"]) in wanted and s["status"] == "confirmed"]
        shard_indexes = [int(s["shard_index"]) for s in target_shards]
        # 实到条数按实际分区明细复算：逐分片读取文件后计数，不直接采信登记计划条数
        per_shard: list[dict[str, Any]] = []
        staging_count = 0
        for shard in target_shards:
            actual_records = sa_svc._read_jsonl(Path(shard["artifact_path"]))
            actual_count = len(actual_records)
            staging_count += actual_count
            per_shard.append({
                "shard_index": int(shard["shard_index"]),
                "manifest_record_count": int(shard["record_count"]),
                "actual_record_count": actual_count,
                "checksum": shard["checksum"],
                "artifact_path": shard["artifact_path"],
                "matches_manifest": actual_count == int(shard["record_count"]),
            })

        missing_count = max(0, expected - staging_count)
        late_count = missing_count
        is_balanced = expected == staging_count and all(p["matches_manifest"] for p in per_shard)

        recon_entry["实到条数"] = str(staging_count)
        recon_entry["迟到条数"] = str(late_count)
        recon_entry["缺失条数"] = str(missing_count)
        recon_entry["清单编号"] = manifest["manifest_code"]
        recon_entry["暂存代际"] = str(manifest["generation"])
        recon_entry["消费分片"] = json.dumps(
            {"generation": int(manifest["generation"]),
             "consumable_shard_indexes": shard_indexes,
             "planned_shard_indexes": sorted(int(s["shard_index"]) for s in manifest["shards"])},
            ensure_ascii=False)
        source_batch_info = {
            "batch_code": batch_code,
            "source_batch_file": manifest.get("source_batch_file"),
            "manifest_code": manifest["manifest_code"],
            "generation": int(manifest["generation"]),
            "planned_total_records": manifest["planned_total_records"],
            "shard_size": manifest["shard_size"],
        }
        recon_entry["源批信息"] = json.dumps(source_batch_info, ensure_ascii=False)

        result_action = "确认平账" if is_balanced else "标记差异"
        self.run_action(int(recon_entry["id"]), result_action)

        # 生成业务回执：仅在真正结算时产生，挂起/恢复期间无回执
        receipt_code = f"RCPT-{recon_entry['对账编号']}"
        receipt = {
            "receipt_code": receipt_code,
            "recon_code": recon_entry["对账编号"],
            "batch_code": batch_code,
            "manifest_code": manifest["manifest_code"],
            "generation": int(manifest["generation"]),
            "trigger": trigger,
            "expected": expected,
            "actual": staging_count,
            "late": late_count,
            "missing": missing_count,
            "balanced": is_balanced,
            "consumed_shard_indexes": shard_indexes,
            "per_shard": per_shard,
            "source_batch_info": source_batch_info,
            "issued_at": time.time(),
        }
        self._atomic_write_json(
            self._data_dir("receipts") / f"{receipt_code}.json", receipt)
        self._write_jsonl(
            self._data_dir("events") / "reconcile_receipt_log.jsonl",
            [{"action": "issue_receipt", **{k: receipt[k] for k in (
                "receipt_code", "recon_code", "batch_code", "manifest_code", "generation",
                "trigger", "expected", "actual", "balanced")}, "at": time.time()}],
        )
        recon_entry["回执编号"] = receipt_code

        self._write_jsonl(
            self._data_dir("events") / "reconcile_log.jsonl",
            [{"action": "reconcile", "batch_code": batch_code, "expected": expected,
              "actual": staging_count, "late": late_count, "missing": missing_count,
              "balanced": is_balanced, "generation": int(manifest["generation"]),
              "manifest_code": manifest["manifest_code"], "trigger": trigger,
              "reconciled_at": time.time()}],
        )

        result = {
            "recon_code": recon_entry["对账编号"],
            "batch_code": batch_code,
            "expected": expected,
            "actual": staging_count,
            "late": late_count,
            "missing": missing_count,
            "balanced": is_balanced,
            "generation": int(manifest["generation"]),
            "manifest_code": manifest["manifest_code"],
            "consumed_shard_indexes": shard_indexes,
            "per_shard": per_shard,
            "receipt_code": receipt_code,
        }

        # 对账结果驱动时效监控指标更新
        if late_count > 0 or not is_balanced:
            try:
                sm_svc = self._safe_import("app.services.sla_monitor", "SlaMonitorService")()
                sm_result = sm_svc.update_arrival_stats(batch_code, late_count, missing_count)
                result["auto_sla_update"] = sm_result
            except Exception as e:
                result["auto_sla_update"] = {"error": str(e)}

        # 对账完成后记录传输血缘
        try:
            tl_svc = self._safe_import("app.services.transfer_lineage", "TransferLineageService")()
            lineage_result = tl_svc.record_lineage(batch_code, is_balanced)
            result["auto_lineage"] = lineage_result
        except Exception as e:
            result["auto_lineage"] = {"error": str(e)}

        return result

    def _settle_without_manifest(self, recon_entry: dict[str, Any], expected: int) -> dict[str, Any]:
        """无任何落地清单的历史批次回退路径：实到按 0 计，仍出具回执并驱动 SLA/血缘。"""
        batch_code = recon_entry["批次编号"]
        staging_count = 0
        missing_count = expected
        late_count = expected
        is_balanced = False
        recon_entry["实到条数"] = str(staging_count)
        recon_entry["迟到条数"] = str(late_count)
        recon_entry["缺失条数"] = str(missing_count)
        recon_entry["消费分片"] = json.dumps(
            {"generation": None, "consumable_shard_indexes": [], "planned_shard_indexes": []},
            ensure_ascii=False)
        recon_entry["源批信息"] = json.dumps(
            {"batch_code": batch_code, "manifest_code": None}, ensure_ascii=False)
        self.run_action(int(recon_entry["id"]), "标记差异")

        receipt_code = f"RCPT-{recon_entry['对账编号']}"
        receipt = {
            "receipt_code": receipt_code,
            "recon_code": recon_entry["对账编号"],
            "batch_code": batch_code,
            "manifest_code": None,
            "generation": None,
            "trigger": "execute",
            "expected": expected,
            "actual": staging_count,
            "late": late_count,
            "missing": missing_count,
            "balanced": is_balanced,
            "consumed_shard_indexes": [],
            "per_shard": [],
            "issued_at": time.time(),
        }
        self._atomic_write_json(self._data_dir("receipts") / f"{receipt_code}.json", receipt)
        recon_entry["回执编号"] = receipt_code
        self._write_jsonl(
            self._data_dir("events") / "reconcile_log.jsonl",
            [{"action": "reconcile", "batch_code": batch_code, "expected": expected,
              "actual": staging_count, "late": late_count, "missing": missing_count,
              "balanced": is_balanced, "generation": None, "trigger": "execute",
              "reconciled_at": time.time()}],
        )

        result = {
            "recon_code": recon_entry["对账编号"],
            "batch_code": batch_code,
            "expected": expected,
            "actual": staging_count,
            "late": late_count,
            "missing": missing_count,
            "balanced": is_balanced,
            "generation": None,
            "manifest_code": None,
            "consumed_shard_indexes": [],
            "per_shard": [],
            "receipt_code": receipt_code,
        }
        try:
            sm_svc = self._safe_import("app.services.sla_monitor", "SlaMonitorService")()
            result["auto_sla_update"] = sm_svc.update_arrival_stats(batch_code, late_count, missing_count)
        except Exception as e:
            result["auto_sla_update"] = {"error": str(e)}
        try:
            tl_svc = self._safe_import("app.services.transfer_lineage", "TransferLineageService")()
            result["auto_lineage"] = tl_svc.record_lineage(batch_code, is_balanced)
        except Exception as e:
            result["auto_lineage"] = {"error": str(e)}
        return result

    # -- 中断恢复：清单补齐后自动续做 --

    def resume_pending_reconciliations(self, batch_code: str) -> dict[str, Any]:
        """落地恢复入口调用：把该批次所有「待清单确认」的对账按补齐后的清单自动续做。

        恢复期间（清单仍 partial）这些请求不会形成业务回执；只有清单确认后续做成功才发回执。
        """
        sa_svc = self._staging_service()
        pending = [
            row for row in store.rows(self.MODULE)
            if row.get("批次编号") == batch_code and row.get("status") == "待清单确认"
        ]
        resumed, still_waiting = [], []
        for row in pending:
            generation = int(row["暂存代际"]) if row.get("暂存代际") else None
            manifest = None
            if generation is not None:
                candidate = sa_svc.get_manifest(batch_code, generation)
                if candidate and candidate.get("status") == "confirmed" and sa_svc._manifest_readable(candidate):
                    manifest = candidate
            if manifest is None:
                still_waiting.append({"recon_id": row["id"], "recon_code": row["对账编号"],
                                      "generation": generation})
                continue
            result = self._settle(
                row, manifest, int(row.get("应到条数", 0)), trigger="resume",
                shard_scope=self._entry_shard_scope(row))
            resumed.append(result)
        self._write_jsonl(
            self._data_dir("events") / "reconcile_resume_log.jsonl",
            [{"action": "resume_reconciliations", "batch_code": batch_code,
              "resumed_codes": [r["recon_code"] for r in resumed],
              "still_waiting": still_waiting, "at": time.time()}],
        )
        return {"batch_code": batch_code, "resumed": resumed, "still_waiting": still_waiting}

    def get_receipt(self, recon_id: int) -> dict[str, Any] | None:
        """读取对账业务回执；挂起或未结算的请求返回 None。"""
        entry = self.get_entry(recon_id)
        if entry is None or not entry.get("回执编号"):
            return None
        receipt_file = self._data_dir("receipts") / f"{entry['回执编号']}.json"
        if not receipt_file.exists():
            return None
        with open(receipt_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # -- 余量另开对账 --

    def plan_remainder_reconciliations(self, batch_code: str) -> dict[str, Any]:
        """按实际分区明细复算各代际尚未被任何已结算对账覆盖的分片，为余量另开对账。

        已结算对账消费过的 (代际, 分片) 取并集；每个清单分片集合减去该并集后若仍有剩余，
        则以剩余分片的实际记录数之和作为应到条数创建新对账请求，确保末片补齐后的数据
        有独立对账承接，不与早期部分范围对账混算。
        """
        sa_svc = self._staging_service()
        manifests = sa_svc.list_manifests(batch_code)
        settled = [
            row for row in store.rows(self.MODULE)
            if row.get("批次编号") == batch_code
            and row.get("status") in ("已平账", "存在差异")
            and row.get("消费分片")
        ]
        covered: dict[int, set[int]] = {}
        for row in settled:
            try:
                scope = json.loads(row["消费分片"])
            except json.JSONDecodeError:
                continue
            gen = int(scope.get("generation"))
            covered.setdefault(gen, set()).update(
                int(i) for i in scope.get("consumable_shard_indexes", []))

        plans: list[dict[str, Any]] = []
        for manifest in manifests:
            gen = int(manifest["generation"])
            all_shards = {int(s["shard_index"]) for s in manifest["shards"]
                          if s["status"] == "confirmed"}
            uncovered = sorted(all_shards - covered.get(gen, set()))
            if not uncovered:
                continue
            remainder_records = sum(
                int(s["record_count"]) for s in manifest["shards"]
                if int(s["shard_index"]) in set(uncovered))
            created = self.create_reconciliation(
                batch_code, remainder_records, generation=gen, shard_scope=uncovered)
            plans.append({
                "recon_id": created["recon_id"],
                "recon_code": created["recon_code"],
                "generation": gen,
                "uncovered_shard_indexes": uncovered,
                "expected_count": remainder_records,
            })

        self._write_jsonl(
            self._data_dir("events") / "reconcile_remainder_log.jsonl",
            [{"action": "plan_remainder", "batch_code": batch_code, "plans": plans,
              "at": time.time()}],
        )
        return {"batch_code": batch_code, "remainder_plans": plans,
                "covered_by_generation": {str(gen): sorted(idx) for gen, idx in covered.items()}}

    def get_pending_reconciliations(self, batch_code: str = "") -> list[dict[str, Any]]:
        """获取待清单确认的对账请求及其消费范围，供工作台展示「待新清单确认后自动续做」。"""
        rows = [r for r in store.rows(self.MODULE) if r.get("status") == "待清单确认"]
        if batch_code:
            rows = [r for r in rows if r.get("批次编号") == batch_code]
        result = []
        for row in rows:
            scope = {}
            if row.get("消费分片"):
                try:
                    scope = json.loads(row["消费分片"])
                except json.JSONDecodeError:
                    scope = {}
            result.append({
                "recon_id": row["id"],
                "recon_code": row["对账编号"],
                "batch_code": row["批次编号"],
                "generation": int(row["暂存代际"]) if row.get("暂存代际") else None,
                "manifest_code": row.get("清单编号"),
                "planned_shard_indexes": scope.get("planned_shard_indexes", []),
                "consumable_shard_indexes": scope.get("consumable_shard_indexes", []),
            })
        return result

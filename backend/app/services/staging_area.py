"""暂存落地领域服务：维护传输数据的暂存分区落地路径、分片文件组织、可确认落地清单与驻留时限。

同一批次重跑落地时会产生两代暂存数据：每次落地占用独立的代际目录，旧代产物不会被覆盖，
因此仍可完整读取。每个分片先原子写入独立分片文件，再登记进可确认落地清单（manifest）；
只有进入清单且状态为 confirmed 的分片才允许对账消费，确保对账读到的记录范围与同一版本
实际落地的数据严格对应。清单整体原子提交，落地中断后可凭断点与清单恢复并辨明已完成分区。
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StagingAreaService(BaseModuleService):
    MODULE = "staging_area"
    ENTITY = "暂存分区"
    OPERATION_TERM = "水位"
    LIST_FIELDS = [
        "暂存编号", "批次编号", "落地路径", "代际", "清单编号", "文件数量", "记录条数",
        "占用字节", "分片规划", "已确认分片", "驻留时限", "落地水位", "清单状态",
    ]
    REQUIRED_FIELDS = ["暂存编号", "批次编号", "落地路径"]
    STATUS_ORDER = ["草稿", "落地中", "待清单确认", "已落地", "已清空"]
    ACTION_RULES = {
        "启动落地": "落地中",
        "确认落地": "已落地",
        "执行清空": "已清空",
    }
    NEGATIVE_ACTIONS = ["执行清空"]

    # -- 代际、清单与分片定位 --

    def _generations_of_batch(self, batch_code: str) -> list[dict[str, Any]]:
        """返回同一批次现存暂存分区（代际），按代际号升序；已清空的分区不再参与读取。"""
        rows = [
            row for row in store.rows(self.MODULE)
            if row.get("批次编号") == batch_code and row.get("status") != "已清空"
        ]
        return sorted(rows, key=lambda row: int(row.get("代际") or 0))

    def _next_generation(self, batch_code: str) -> int:
        """同批次重跑落地时分配新代际号，保证两代暂存数据并存、互不覆盖。"""
        existing = self._generations_of_batch(batch_code)
        return max((int(row.get("代际") or 0) for row in existing), default=0) + 1

    def _generation_dir(self, batch_code: str, generation: int) -> Path:
        """代际目录：staging/<批次>/gen<代际>/，重跑写入新目录，旧代目录保持完整可读。"""
        return self._data_dir("staging") / batch_code / f"gen{generation}"

    def _manifest_file(self, batch_code: str, generation: int) -> Path:
        return self._generation_dir(batch_code, generation) / "manifest.json"

    def _shard_file(self, batch_code: str, generation: int, shard_index: int) -> Path:
        return self._generation_dir(batch_code, generation) / f"shard_{shard_index:04d}.jsonl"

    def _load_manifest(self, batch_code: str, generation: int) -> dict[str, Any] | None:
        """读取可确认落地清单；清单文件缺失或损坏返回 None（半份清单不可能出现，因为原子提交）。"""
        manifest_file = self._manifest_file(batch_code, generation)
        if not manifest_file.exists():
            return None
        with open(manifest_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_manifest(self, manifest: dict[str, Any]) -> Path:
        """原子提交清单，任何时刻读到的清单都是完整的上一版或新版。"""
        path = self._manifest_file(manifest["batch_code"], int(manifest["generation"]))
        manifest["updated_at"] = time.time()
        self._atomic_write_json(path, manifest)
        return path

    def _find_store_entry(self, batch_code: str, generation: int) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("批次编号") == batch_code and str(row.get("代际")) == str(generation):
                return row
        return None

    def _sync_entry_from_manifest(self, entry: dict[str, Any], manifest: dict[str, Any]) -> None:
        """把清单权威状态同步到工作台展示字段，值班人员据此分辨已完成分区与待确认分区。"""
        shards = manifest["shards"]
        confirmed = [s for s in shards if s["status"] == "confirmed"]
        entry["文件数量"] = str(len(confirmed))
        entry["记录条数"] = str(manifest["planned_total_records"])
        entry["分片规划"] = str(manifest["planned_shard_count"])
        entry["已确认分片"] = str(len(confirmed))
        entry["落地水位"] = str(sum(int(s["record_count"]) for s in confirmed))
        entry["占用字节"] = str(sum(int(s["bytes"]) for s in confirmed))
        entry["清单状态"] = manifest["status"]

    def _refresh_entry_status(self, entry: dict[str, Any], manifest: dict[str, Any]) -> None:
        """根据清单状态推进分区状态：清单未确认齐=落地中（中断态），全部确认=待清单确认。"""
        self._sync_entry_from_manifest(entry, manifest)
        if manifest["status"] == "confirmed":
            entry["status"] = "待清单确认"
            entry["pending"] = True
            entry["abnormal"] = False
        elif entry["status"] not in ("已清空",):
            entry["status"] = "落地中"
            entry["pending"] = True

    # -- 分片落地写入 --

    def land_batch(
        self,
        batch_code: str,
        record_count: int,
        shard_size: int = 10,
        pause_before_last_shard: bool = False,
        generation: int | None = None,
    ) -> dict[str, Any]:
        """将传输批次数据按分片落地到新一代暂存分区，并为可读分片登记可确认落地清单。

        每个分片先原子写入独立文件，再以 confirmed 状态登记进清单（清单原子提交），
        因此下游对账永远只会读到完整分片。pause_before_last_shard 用于模拟落地中断：
        最后一片保持 planned 不写、清单停留在 partial，重跑时从断点恢复。
        """
        generation = int(generation) if generation is not None else self._next_generation(batch_code)
        staging_code = f"STAG-{batch_code}-G{generation}"
        manifest_code = f"MANI-{batch_code}-G{generation}"
        landing_path = str(self._generation_dir(batch_code, generation)) + "/"

        values = {
            "暂存编号": staging_code,
            "批次编号": batch_code,
            "落地路径": landing_path,
            "代际": str(generation),
            "清单编号": manifest_code,
            "文件数量": "0",
            "记录条数": str(record_count),
            "占用字节": "0",
            "分片规划": "0",
            "已确认分片": "0",
            "驻留时限": "86400",
            "落地水位": "0",
            "清单状态": "partial",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建暂存分区失败: {missing}")
        self.run_action(entry["id"], "启动落地")

        # 读取上游传输批次产物文件，缺失时按登记条数模拟，模拟数据同样带行号可复算
        source_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        if source_file.exists():
            records = self._read_jsonl(source_file)
        else:
            records = self._simulate_records(batch_code, count=record_count, field_prefix="staging")

        planned_shard_count = max(1, (len(records) + shard_size - 1) // shard_size)
        manifest: dict[str, Any] = {
            "manifest_code": manifest_code,
            "staging_code": staging_code,
            "batch_code": batch_code,
            "generation": generation,
            "source_batch_file": str(source_file),
            "planned_total_records": len(records),
            "planned_shard_count": planned_shard_count,
            "shard_size": shard_size,
            "status": "partial",
            "paused_before_last_shard": bool(pause_before_last_shard),
            "shards": [],
            "created_at": time.time(),
        }

        # 先落一份 partial 清单，声明规划条数与分片，即使立刻中断也能辨明该分区身份
        self._save_manifest(manifest)
        self._write_checkpoint(f"staging_{staging_code}", {
            "staging_code": staging_code,
            "manifest_code": manifest_code,
            "batch_code": batch_code,
            "generation": generation,
            "planned_shard_count": planned_shard_count,
            "planned_total_records": len(records),
            "confirmed_shards": 0,
            "paused_before_last_shard": bool(pause_before_last_shard),
            "landed_at": time.time(),
            "retention_seconds": 86400,
        })

        paused = False
        for shard_index in range(planned_shard_count):
            is_last = shard_index == planned_shard_count - 1
            if pause_before_last_shard and is_last:
                # 模拟落地中断：末片只登记计划、不写文件，清单保持 partial
                manifest["shards"].append({
                    "shard_index": shard_index,
                    "status": "planned",
                    "record_count": 0,
                    "bytes": 0,
                    "artifact_path": str(self._shard_file(batch_code, generation, shard_index)),
                    "checksum": "",
                    "confirmed_at": None,
                })
                paused = True
                continue
            self._write_shard(manifest, records, shard_index, shard_size)

        if not paused:
            manifest["status"] = "confirmed"
            manifest["paused_before_last_shard"] = False
        self._save_manifest(manifest)
        self._refresh_entry_status(entry, manifest)

        # 刷新断点，恢复入口据此判断从哪个分片续做
        self._write_checkpoint(f"staging_{staging_code}", {
            "staging_code": staging_code,
            "manifest_code": manifest_code,
            "batch_code": batch_code,
            "generation": generation,
            "planned_shard_count": planned_shard_count,
            "planned_total_records": len(records),
            "confirmed_shards": len([s for s in manifest["shards"] if s["status"] == "confirmed"]),
            "paused_before_last_shard": paused,
            "landed_at": time.time(),
            "retention_seconds": 86400,
        })

        return {
            "staging_code": staging_code,
            "manifest_code": manifest_code,
            "batch_code": batch_code,
            "generation": generation,
            "records_landed": int(entry["落地水位"]),
            "planned_total_records": manifest["planned_total_records"],
            "planned_shard_count": planned_shard_count,
            "confirmed_shards": len([s for s in manifest["shards"] if s["status"] == "confirmed"]),
            "paused": paused,
            "manifest_status": manifest["status"],
            "artifact_path": landing_path,
            "manifest_path": str(self._manifest_file(batch_code, generation)),
        }

    def _write_shard(
        self,
        manifest: dict[str, Any],
        records: list[dict[str, Any]],
        shard_index: int,
        shard_size: int,
    ) -> dict[str, Any]:
        """写单个分片：原子写分片文件 -> 登记 confirmed 清单条目 -> 原子提交清单。"""
        batch_code = manifest["batch_code"]
        generation = int(manifest["generation"])
        chunk = records[shard_index * shard_size:(shard_index + 1) * shard_size]
        shard_file = self._shard_file(batch_code, generation, shard_index)
        self._atomic_write_jsonl(shard_file, chunk)
        shard_bytes = sum(
            len(json.dumps(r, ensure_ascii=False, default=str).encode("utf-8")) for r in chunk
        )
        shard_info = {
            "shard_index": shard_index,
            "status": "confirmed",
            "record_count": len(chunk),
            "bytes": shard_bytes,
            "artifact_path": str(shard_file),
            "checksum": self._compute_checksum(json.dumps(chunk, ensure_ascii=False, default=str)),
            "confirmed_at": time.time(),
        }
        existing = {s["shard_index"]: s for s in manifest["shards"]}
        if shard_index in existing:
            manifest["shards"] = [
                shard_info if s["shard_index"] == shard_index else s for s in manifest["shards"]
            ]
        else:
            manifest["shards"].append(shard_info)
        manifest["shards"].sort(key=lambda s: s["shard_index"])
        self._save_manifest(manifest)
        return shard_info

    def resume_landing(self, batch_code: str, generation: int | None = None) -> dict[str, Any]:
        """落地中断后从对账入口/工作台恢复：补齐未确认分片并提交完整清单。

        依据断点与清单识别中断分区身份，重写所有 planned 分片（已确认分片不重写、不覆盖），
        成功后清单转为 confirmed。同一批次若存在多个中断代际，默认恢复代际号最大的一个。
        """
        target_generation = generation
        if target_generation is None:
            partial = [
                int(row.get("代际") or 0)
                for row in store.rows(self.MODULE)
                if row.get("批次编号") == batch_code and row.get("清单状态") == "partial"
            ]
            if not partial:
                return {"error": f"批次 {batch_code} 没有待恢复的暂存分区", "batch_code": batch_code}
            target_generation = max(partial)

        manifest = self._load_manifest(batch_code, target_generation)
        entry = self._find_store_entry(batch_code, target_generation)
        if manifest is None or entry is None:
            return {"error": f"批次 {batch_code} 代际 {target_generation} 的清单或分区不存在",
                    "batch_code": batch_code, "generation": target_generation}

        source_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        if source_file.exists():
            records = self._read_jsonl(source_file)
        else:
            records = self._simulate_records(
                batch_code, count=int(manifest["planned_total_records"]), field_prefix="staging")

        shard_size = int(manifest["shard_size"])
        resumed_shards: list[int] = []
        for shard in manifest["shards"]:
            if shard["status"] != "confirmed":
                self._write_shard(manifest, records, int(shard["shard_index"]), shard_size)
                resumed_shards.append(int(shard["shard_index"]))

        manifest["status"] = "confirmed"
        manifest["paused_before_last_shard"] = False
        self._save_manifest(manifest)
        self._refresh_entry_status(entry, manifest)

        self._write_checkpoint(
            f"staging_{manifest['staging_code']}",
            {
                "staging_code": manifest["staging_code"],
                "manifest_code": manifest["manifest_code"],
                "batch_code": batch_code,
                "generation": target_generation,
                "planned_shard_count": manifest["planned_shard_count"],
                "planned_total_records": manifest["planned_total_records"],
                "confirmed_shards": len(manifest["shards"]),
                "paused_before_last_shard": False,
                "resumed_shards": resumed_shards,
                "resumed_at": time.time(),
                "retention_seconds": 86400,
            },
        )
        self._write_jsonl(
            self._data_dir("events") / "staging_resume_log.jsonl",
            [{"action": "resume_landing", "batch_code": batch_code, "generation": target_generation,
              "resumed_shards": resumed_shards, "manifest_status": "confirmed", "at": time.time()}],
        )
        return {
            "staging_code": manifest["staging_code"],
            "manifest_code": manifest["manifest_code"],
            "batch_code": batch_code,
            "generation": target_generation,
            "resumed_shards": resumed_shards,
            "confirmed_shards": len(manifest["shards"]),
            "planned_shard_count": manifest["planned_shard_count"],
            "records_landed": int(entry["落地水位"]),
            "manifest_status": "confirmed",
        }

    # -- 对账消费侧查询 --

    def get_manifest(self, batch_code: str, generation: int) -> dict[str, Any] | None:
        """对外暴露指定代际的可确认落地清单。"""
        return self._load_manifest(batch_code, generation)

    def list_manifests(self, batch_code: str) -> list[dict[str, Any]]:
        """列出同一批次全部代际清单（含 partial），供恢复入口辨明已完成分区是谁。"""
        result = []
        for row in self._generations_of_batch(batch_code):
            gen = int(row.get("代际") or 0)
            manifest = self._load_manifest(batch_code, gen)
            if manifest is not None:
                result.append(manifest)
        return sorted(result, key=lambda m: int(m["generation"]))

    def latest_confirmed_manifest(self, batch_code: str) -> dict[str, Any] | None:
        """返回同批次代际号最大、清单已全部确认且可完整读取的分区。"""
        candidates = [
            m for m in self.list_manifests(batch_code)
            if m.get("status") == "confirmed" and self._manifest_readable(m)
        ]
        return candidates[-1] if candidates else None

    def _manifest_readable(self, manifest: dict[str, Any]) -> bool:
        """清单中每个 confirmed 分片文件必须真实存在且可解析，否则该清单不可消费。"""
        for shard in manifest["shards"]:
            if shard["status"] != "confirmed":
                return False
            path = Path(shard["artifact_path"])
            if not path.exists():
                return False
            if len(self._read_jsonl(path)) != int(shard["record_count"]):
                return False
        return True

    def read_manifest_records(
        self, manifest: dict[str, Any], shard_indexes: list[int] | None = None
    ) -> list[dict[str, Any]]:
        """按清单读取分片集合的实际记录，条数与清单同一版本的分片严格对应。"""
        wanted = set(shard_indexes) if shard_indexes is not None else None
        records: list[dict[str, Any]] = []
        for shard in sorted(manifest["shards"], key=lambda s: int(s["shard_index"])):
            if shard["status"] != "confirmed":
                continue
            if wanted is not None and int(shard["shard_index"]) not in wanted:
                continue
            records.extend(self._read_jsonl(Path(shard["artifact_path"])))
        return records

    def recovery_view(self, batch_code: str) -> dict[str, Any]:
        """落地中断后重新进入工作台的恢复视图：已完成分区、中断分区及其清单/分片明细。"""
        partitions = []
        for row in self._generations_of_batch(batch_code):
            gen = int(row.get("代际") or 0)
            manifest = self._load_manifest(batch_code, gen)
            partitions.append({
                "generation": gen,
                "staging_code": row.get("暂存编号"),
                "manifest_code": row.get("清单编号"),
                "entry_status": row.get("status"),
                "manifest_status": manifest.get("status") if manifest else "missing",
                "planned_shard_count": manifest.get("planned_shard_count") if manifest else 0,
                "confirmed_shards": len([s for s in (manifest or {}).get("shards", [])
                                         if s["status"] == "confirmed"]) if manifest else 0,
                "confirmed_records": int(row.get("落地水位") or 0),
                "readable": bool(manifest and self._manifest_readable(manifest)),
                "shard_indexes": [int(s["shard_index"]) for s in (manifest or {}).get("shards", [])],
                "confirmed_shard_indexes": [
                    int(s["shard_index"]) for s in (manifest or {}).get("shards", [])
                    if s["status"] == "confirmed"
                ],
            })
        return {"batch_code": batch_code, "partitions": partitions}

    # -- 既有工作台动作 --

    def confirm_land(self, staging_id: int) -> dict[str, Any]:
        """确认暂存分区落地完成：以清单为准校验分片完整性并写入落地确认日志。"""
        entry, msg = self.run_action(staging_id, "确认落地")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        batch_code = entry["批次编号"]
        generation = int(entry.get("代际") or 0)
        manifest = self._load_manifest(batch_code, generation)
        if manifest is None or not self._manifest_readable(manifest):
            return {"error": "清单分片不完整或不可读，不能确认落地", "staging_id": staging_id,
                    "manifest_code": entry.get("清单编号")}
        manifest["status"] = "confirmed"
        self._save_manifest(manifest)
        records = self.read_manifest_records(manifest)
        checksum = self._compute_checksum(json.dumps(records, ensure_ascii=False, default=str))
        entry["清单状态"] = "confirmed"
        self._write_jsonl(
            self._data_dir("events") / "staging_confirm_log.jsonl",
            [{"action": "confirm_land", "staging_code": entry["暂存编号"],
              "manifest_code": manifest["manifest_code"], "generation": generation,
              "record_count": len(records), "shard_count": len(manifest["shards"]),
              "checksum": checksum, "confirmed_at": time.time()}],
        )
        return {"staging_code": entry["暂存编号"], "manifest_code": manifest["manifest_code"],
                "status": "已落地", "record_count": len(records),
                "shard_count": len(manifest["shards"]), "checksum": checksum}

    def clear_staging(self, staging_id: int) -> dict[str, Any]:
        """执行暂存分区清空，登记清空断点与日志（代际产物目录保留给在途对账，清单标记废弃）。"""
        entry, msg = self.run_action(staging_id, "执行清空")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        staging_code = entry["暂存编号"]
        batch_code = entry["批次编号"]
        generation = int(entry.get("代际") or 0)
        manifest = self._load_manifest(batch_code, generation)
        if manifest is not None:
            manifest["status"] = "discarded"
            self._save_manifest(manifest)
            entry["清单状态"] = "discarded"
        self._write_checkpoint(f"staging_{staging_code}_cleared", {
            "staging_code": staging_code,
            "cleared_at": time.time(),
            "status": "cleared",
        })
        self._write_jsonl(
            self._data_dir("events") / "staging_clear_log.jsonl",
            [{"action": "clear", "staging_code": staging_code, "cleared_at": time.time()}],
        )
        return {"staging_code": staging_code, "status": "已清空"}

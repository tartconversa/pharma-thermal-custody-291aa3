"""冷链数据仓：持久化原始文件、版本化事实与集市、查询与复核引用。

写入串行（每次接收一个新不可变版本），读取走只读连接，天然满足
“仓库更新期间仍有查询、同一查询属于同一版”。
"""
from __future__ import annotations

import csv
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import schema
from .engine import (
    Correction,
    Engine,
    Flow,
    Load,
    Lot,
    Reading,
    Receipt,
    Rule,
)
from .times import UTC, day8, parse_dt, split_by_midnight

STATUSES = ("normal", "excursion", "conflict", "unknown")


def _now() -> datetime:
    return datetime.now(tz=UTC)


class Warehouse:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.raw_dir = self.path.parent / "raw_files"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.path))
        for stmt in schema.SCHEMA_STATEMENTS:
            self.con.execute(stmt)

    def close(self):
        self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ============================================================ 入仓

    def ingest_pack(self, pack_dir: str | Path, received_at: str,
                    note: str | None = None, fail_after_files: int | None = None) -> dict:
        """接收一个资料包目录（含若干 <table>.csv）。

        - 文件按内容 SHA256 去重，相同摘要不增加事实；
        - 同业务键的新行整行修订，旧行保留在 raw_rows；
        - fail_after_files 为测试用故障注入：处理指定文件数后中断，
          整个批次回滚，重跑安全（中途停止后继续）。
        """
        pack_dir = Path(pack_dir)
        received = parse_dt(received_at)
        batch_id = "B-" + uuid.uuid4().hex[:12]
        files = sorted(p for p in pack_dir.glob("*.csv")
                       if p.stem in schema.RAW_TABLE_KEYS)
        if not files:
            raise ValueError(f"目录中没有可识别的资料表：{pack_dir}")

        applied, duplicates = [], []
        # 先做内容寻址归档（幂等，崩溃后重跑不产生重复）
        staged = []
        for fp in files:
            data = fp.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            archived = self.raw_dir / f"{digest}.csv"
            if not archived.exists():
                archived.write_bytes(data)
            staged.append((fp, digest, len(data), archived))

        try:
            self.con.execute("BEGIN TRANSACTION")
            self.con.execute(
                "INSERT INTO ingest_batches VALUES (?, ?, ?)",
                [batch_id, received, note],
            )
            seq_start = self._next_raw_seq()
            batch_seq = 0
            processed = 0
            for fp, digest, size, archived in staged:
                table_name = fp.stem
                file_id = "F-" + uuid.uuid4().hex[:12]
                dup = self.con.execute(
                    "SELECT 1 FROM ingest_files WHERE sha256 = ? LIMIT 1",
                    [digest],
                ).fetchone()
                outcome = "duplicate" if dup else "applied"
                # 先登记文件（raw_rows 对其有外键），再写原始行
                self.con.execute(
                    "INSERT INTO ingest_files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [file_id, batch_id, table_name,
                     fp.name, digest, size, _now(),
                     self._count_data_rows(archived), outcome],
                )
                if outcome == "applied":
                    with archived.open(encoding="utf-8", newline="") as stream:
                        reader = csv.DictReader(stream)
                        key_col = schema.RAW_TABLE_KEYS[table_name]
                        rows = list(reader)
                    for row in rows:
                        batch_seq += 1
                        record_key = row[key_col]
                        self.con.execute(
                            "INSERT INTO raw_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            [seq_start, file_id, batch_id, batch_seq, table_name,
                             record_key, json.dumps(row, ensure_ascii=False), received],
                        )
                        seq_start += 1
                    processed += 1
                else:
                    duplicates.append(fp.name)
                applied.append((fp.name, outcome))
                if fail_after_files is not None and processed >= fail_after_files:
                    raise RuntimeError("故障注入：模拟接收中途停止")
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise

        result = {"batch_id": batch_id, "files": applied, "duplicates": duplicates}
        if processed == 0:
            # 整包内容摘要均已见过：接收历史保留，但不增加事实、不产生新版本
            result.update({
                "version_id": self.current_version(),
                "basis": "new_files",
                "affected_flows": [],
                "full_rebuild": False,
                "reason": "全部文件重复，未增加事实",
            })
            return result
        rebuilt = self.rebuild_version(
            basis="new_files", batch_ids=[batch_id], note=note,
            received=received,
        )
        result.update(rebuilt)
        return result

    def _count_data_rows(self, path: Path) -> int:
        with path.open(encoding="utf-8", newline="") as stream:
            return sum(1 for _ in csv.DictReader(stream))

    def _next_raw_seq(self) -> int:
        row = self.con.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM raw_rows").fetchone()
        return int(row[0])

    # ============================================================ 版本重建

    def _active_rows(self, table_name: str) -> list[dict]:
        """取当前生效行：同业务键按接收批次次序取最后一条。"""
        rows = self.con.execute(
            """
            SELECT row_json FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY table_name, record_key
                    ORDER BY batch_seq, seq
                ) AS rn
                FROM raw_rows
                WHERE table_name = ?
            ) WHERE rn = 1
            """,
            [table_name],
        ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def _load_domain(self):
        lots = [Lot(r["lot_id"], r["sku"], int(r["quantity"]))
                for r in self._active_rows("lots")]
        flows = [
            Flow(r["flow_id"], r["lot_id"], r["parent_id"] or None,
                 int(r["quantity"]), parse_dt(r["split_at"]) if r["split_at"] else None)
            for r in self._active_rows("flows")
        ]
        receipts = [
            Receipt(r["receipt_id"], r["transfer_id"], r["flow_id"], r["side"],
                    r["party"], r["counterparty"], int(r["quantity"]),
                    parse_dt(r["occurred_at"]))
            for r in self._active_rows("handoffs")
        ]
        loads = [
            Load(r["load_id"], r["logger_id"], r["flow_id"],
                 parse_dt(r["start"]), parse_dt(r["end"]))
            for r in self._active_rows("logger_loads")
        ]
        readings = [
            Reading(r["reading_id"], r["logger_id"], parse_dt(r["at"]),
                    float(r["value"]), r["unit"], int(r["coverage_minutes"]))
            for r in self._active_rows("readings")
        ]
        rules = [
            Rule(r["rule_id"], r["sku"], parse_dt(r["start"]), parse_dt(r["end"]),
                 float(r["lower_c"]), float(r["upper_c"]))
            for r in self._active_rows("temperature_rules")
        ]
        corrections = [
            Correction(r["correction_id"], r["receipt_id"],
                       parse_dt(r["new_at"]), parse_dt(r["received_at"]))
            for r in self._active_rows("time_corrections")
        ]
        return lots, flows, receipts, loads, readings, rules, corrections

    def _affected_flows(self) -> tuple[set[str], bool]:
        """根据本批新增的原始行确定受影响支流；无法定位时全量重建。

        新文件只影响与其有关系的货物与区段：
        - lots / temperature_rules / flows：口径或实体变化，全量重建；
        - handoffs / time_corrections：定位到回执所属支流；
        - logger_loads：定位到装载所属支流；
        - readings：经仪器装载反查支流（不为一箱重扫全部仪器历史）。
        """
        new_batches = [
            r[0] for r in self.con.execute(
                "SELECT DISTINCT batch_id FROM raw_rows "
                "WHERE batch_id NOT IN (SELECT UNNEST(batch_ids) FROM versions)"
            ).fetchall()
        ]
        if not new_batches:
            return set(), False
        lots, flows, receipts, loads, readings, _rules, corrections = self._load_domain()
        receipt_flow = {r.receipt_id: r.flow_id for r in receipts}
        load_flow = {ld.load_id: ld.flow_id for ld in loads}
        logger_flows: dict[str, set[str]] = {}
        for ld in loads:
            logger_flows.setdefault(ld.logger_id, set()).add(ld.flow_id)
        reading_logger = {rd.reading_id: rd.logger_id for rd in readings}
        correction_target = {c.correction_id: c.receipt_id for c in corrections}

        affected: set[str] = set()
        wide = False
        tables = self.con.execute(
            "SELECT DISTINCT table_name FROM raw_rows WHERE batch_id = ANY(?)",
            [new_batches],
        ).fetchall()
        for (table_name,) in tables:
            if table_name in ("lots", "temperature_rules", "flows"):
                wide = True
                continue
            keys = [r[0] for r in self.con.execute(
                "SELECT DISTINCT record_key FROM raw_rows "
                "WHERE table_name = ? AND batch_id = ANY(?)",
                [table_name, new_batches],
            ).fetchall()]
            if table_name == "handoffs":
                affected |= {receipt_flow[k] for k in keys if k in receipt_flow}
            elif table_name == "time_corrections":
                for cid in keys:
                    rid = correction_target.get(cid)
                    if rid in receipt_flow:
                        affected.add(receipt_flow[rid])
            elif table_name == "logger_loads":
                affected |= {load_flow[k] for k in keys if k in load_flow}
            elif table_name == "readings":
                for rid in keys:
                    lg = reading_logger.get(rid)
                    if lg:
                        affected |= logger_flows.get(lg, set())
        if wide:
            return set(), True
        return affected, False

    def rebuild_version(self, basis: str, batch_ids: list[str],
                        note: str | None, received: datetime | None = None) -> dict:
        """生成一个不可变新版本；未受影响支流的事实从旧版本复制。"""
        # 没有任何生效原始行（例如纯重复批次）则不产生新版本
        has_rows = self.con.execute("SELECT 1 FROM raw_rows LIMIT 1").fetchone()
        if not has_rows:
            return {"version_id": None, "affected_flows": [], "reason": "无生效事实"}

        prev = self.current_version()
        affected, wide = self._affected_flows()
        if prev is None:
            wide = True
        version_id = (prev or 0) + 1
        domain = self._load_domain()
        lots, flows, receipts, loads, readings, rules, corrections = domain
        result = Engine(lots, flows, receipts, loads, readings, rules, corrections).run()

        try:
            self.con.execute("BEGIN TRANSACTION")
            self.con.execute(
                "INSERT INTO versions VALUES (?, ?, ?, ?, ?, ?, ?)",
                [version_id, _now(), received or _now(), basis, batch_ids,
                 note, sorted(affected) if not wide else [f.flow_id for f in flows]],
            )
            if prev is not None and not wide:
                self._copy_forward(prev, version_id, affected)
            self._insert_version_facts(version_id, domain, result, affected, wide)
            self.con.execute(
                "INSERT OR REPLACE INTO wh_meta VALUES ('current_version', ?)",
                [str(version_id)],
            )
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return {
            "version_id": version_id,
            "basis": basis,
            "affected_flows": sorted(affected) if not wide else [f.flow_id for f in flows],
            "full_rebuild": wide,
        }

    def _copy_forward(self, prev: int, new: int, affected: set[str]):
        """把旧版本中未受影响支流的事实复制到新版本。"""
        flow_tables = {
            "dim_flows": "flow_id",
            "fact_handoffs": "flow_id",
            "fact_ownership": "flow_id",
            "fact_responsibility": "flow_id",
            "fact_loads": "flow_id",
            "fact_temp_intervals": "flow_id",
            "fact_conflicts": "flow_id",
            "mart_quality_flow": "flow_id",
            "mart_daily_quality": "flow_id",
            "mart_daily_carrier": "flow_id",
            "mart_available": "flow_id",
        }
        affected_list = sorted(affected)
        for table, key in flow_tables.items():
            cols = self._columns(table)
            col_list = ", ".join(cols)
            select_list = ", ".join("?" if c == "version_id" else c for c in cols)
            self.con.execute(
                f"INSERT INTO {table} ({col_list}) "
                f"SELECT {select_list} FROM {table} "
                f"WHERE version_id = ? AND NOT array_contains(?, {key})",
                [new, prev, affected_list],
            )
        # 维度药批：只复制未受影响 lot（受影响 lot 随事实重写）
        affected_lots = {
            f.lot_id for f in self._load_domain()[1] if f.flow_id in affected
        }
        cols = self._columns("dim_lots")
        self.con.execute(
            f"INSERT INTO dim_lots ({', '.join(cols)}) "
            f"SELECT {', '.join('?' if c == 'version_id' else c for c in cols)} "
            "FROM dim_lots WHERE version_id = ? AND NOT array_contains(?, lot_id)",
            [new, prev, sorted(affected_lots)],
        )
        # 质量批次集市：只复制未受影响 lot
        cols = self._columns("mart_quality_lot")
        self.con.execute(
            f"INSERT INTO mart_quality_lot ({', '.join(cols)}) "
            f"SELECT {', '.join('?' if c == 'version_id' else c for c in cols)} "
            "FROM mart_quality_lot WHERE version_id = ? AND NOT array_contains(?, lot_id)",
            [new, prev, sorted(affected_lots)],
        )
        # 承运方集市由受影响支流整体重算（见 _build_marts），复制其余 party
        affected_parties = {
            r.carrier_party for r in self._load_domain_responsibility(prev)
            if r.flow_id in affected
        }
        cols = self._columns("mart_carrier")
        self.con.execute(
            f"INSERT INTO mart_carrier ({', '.join(cols)}) "
            f"SELECT {', '.join('?' if c == 'version_id' else c for c in cols)} "
            "FROM mart_carrier WHERE version_id = ? "
            "AND NOT array_contains(?, carrier_party)",
            [new, prev, sorted(affected_parties)],
        )

    def _load_domain_responsibility(self, version_id: int):
        class _R:
            def __init__(self, flow_id, carrier_party):
                self.flow_id = flow_id
                self.carrier_party = carrier_party

        rows = self.con.execute(
            "SELECT DISTINCT flow_id, carrier_party FROM fact_responsibility "
            "WHERE version_id = ?", [version_id],
        ).fetchall()
        return [_R(f, p) for f, p in rows]

    def _columns(self, table: str) -> list[str]:
        return [r[1] for r in self.con.execute(
            f"PRAGMA table_info('{table}')").fetchall()]

    def _insert_many(self, sql: str, rows: list):
        """空列表跳过，避免 executemany 空参报错。"""
        if rows:
            self.con.executemany(sql, rows)

    def _insert_version_facts(self, version_id, domain, result, affected, wide):
        lots, flows, receipts, loads, readings, rules, corrections = domain
        do_flows = set(self.flows_all(flows)) if wide else affected
        lot_ids = {f.lot_id for f in flows if f.flow_id in do_flows} if not wide else {l.lot_id for l in lots}

        # 维度（全量插入；dim 表小，重建版本统一全量写）
        if wide:
            self._insert_many(
                "INSERT INTO dim_lots VALUES (?, ?, ?, ?)",
                [(version_id, l.lot_id, l.sku, l.quantity) for l in lots],
            )
            self._insert_many(
                "INSERT INTO dim_flows VALUES (?, ?, ?, ?, ?, ?)",
                [(version_id, f.flow_id, f.lot_id, f.parent_id, f.quantity, f.split_at)
                 for f in flows],
            )
        else:
            for l in lots:
                if l.lot_id in lot_ids:
                    self.con.execute(
                        "INSERT INTO dim_lots VALUES (?, ?, ?, ?)",
                        [version_id, l.lot_id, l.sku, l.quantity],
                    )
            for f in flows:
                if f.flow_id in do_flows:
                    self.con.execute(
                        "INSERT INTO dim_flows VALUES (?, ?, ?, ?, ?, ?)",
                        [version_id, f.flow_id, f.lot_id, f.parent_id, f.quantity, f.split_at],
                    )

        # 交接事实
        hf_rows = []
        for h in result.handoffs:
            if not wide and h.flow_id not in do_flows:
                continue
            hf_rows.append((version_id, h.transfer_id, h.flow_id, h.sent_receipt,
                            h.received_receipt, h.sender, h.receiver, h.sent_at,
                            h.received_at, h.effective_received_at, h.sent_qty,
                            h.received_qty, h.matched_qty, h.disputed_qty, h.state,
                            h.correction_id, h.sent_correction_id,
                            h.received_correction_id, h.pending_review, h.pending_reason))
        self._insert_many(
            "INSERT INTO fact_handoffs VALUES (" + ", ".join("?" for _ in range(20)) + ")",
            hf_rows,
        )

        # 归属
        self._insert_many(
            "INSERT INTO fact_ownership VALUES (?, ?, ?, ?, ?, ?)",
            [(version_id, s.flow_id, s.seg_start, s.seg_end, s.holder, s.basis)
             for s in result.ownership if wide or s.flow_id in do_flows],
        )
        # 责任
        self._insert_many(
            "INSERT INTO fact_responsibility VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(version_id, s.flow_id, s.carrier_party, s.seg_start, s.seg_end,
              s.transfer_id, s.end_pending)
             for s in result.responsibility if wide or s.flow_id in do_flows],
        )
        # 装载
        self._insert_many(
            "INSERT INTO fact_loads VALUES (?, ?, ?, ?, ?, ?)",
            [(version_id, ld.load_id, ld.logger_id, ld.flow_id, ld.start, ld.end)
             for ld in loads if wide or ld.flow_id in do_flows],
        )
        # 温控区间
        iv_rows = []
        for iv in result.intervals:
            if not wide and iv.flow_id not in do_flows:
                continue
            iv_rows.append((version_id, iv.flow_id, iv.lot_id, iv.sku,
                            iv.seg_start, iv.seg_end, iv.status, iv.unknown_reason,
                            iv.loggers, json.dumps(iv.evidence, ensure_ascii=False)))
        self._insert_many(
            "INSERT INTO fact_temp_intervals VALUES (" + ", ".join("?" for _ in range(10)) + ")",
            iv_rows,
        )
        # 冲突
        cf_rows = []
        for cf in result.conflicts:
            if not wide and cf.flow_id not in do_flows:
                continue
            cf_rows.append((version_id, cf.flow_id, cf.conflict_start, cf.conflict_end,
                            cf.loggers, json.dumps(cf.detail, ensure_ascii=False)))
        self._insert_many(
            "INSERT INTO fact_conflicts VALUES (?, ?, ?, ?, ?, ?)", cf_rows,
        )

        self._build_marts(version_id, domain, result, do_flows, lot_ids, wide)

    def _build_marts(self, version_id, domain, result, do_flows, lot_ids, wide):
        lots, flows, *_ = domain
        flow_qty = {f.flow_id: f.quantity for f in flows}
        flow_lot = {f.flow_id: f.lot_id for f in flows}

        intervals_affected = [
            iv for iv in result.intervals if wide or iv.flow_id in do_flows
        ]
        # ---- 按支流聚合
        flow_agg = {}
        daily_q, daily_c = [], []
        for iv in intervals_affected:
            qty = flow_qty[iv.flow_id]
            agg = flow_agg.setdefault(iv.flow_id, {s: 0 for s in STATUSES})
            secs = int((iv.seg_end - iv.seg_start).total_seconds())
            agg[iv.status] += secs
            for d, s, e in split_by_midnight(iv.seg_start, iv.seg_end):
                part = int((e - s).total_seconds())
                daily_q.append((version_id, d, iv.lot_id, iv.flow_id, iv.status,
                                part, part * qty))
        # 责任段与区间相交 -> 承运方口径（同一事实，重叠仪器已在区间层并集，不翻倍）
        # 受影响 party 用其【全部】支流重算，保证替换旧版本行后口径完整
        affected_parties = {
            resp.carrier_party for resp in result.responsibility
            if wide or resp.flow_id in do_flows
        }
        carrier_agg: dict[str, dict[str, int]] = {}
        for resp in result.responsibility:
            if not wide and resp.carrier_party not in affected_parties:
                continue
            qty = flow_qty[resp.flow_id]
            for iv in result.intervals:
                if iv.flow_id != resp.flow_id:
                    continue
                s, e = max(iv.seg_start, resp.seg_start), min(iv.seg_end, resp.seg_end)
                if s >= e:
                    continue
                secs = int((e - s).total_seconds())
                a = carrier_agg.setdefault(resp.carrier_party, {x: 0 for x in STATUSES})
                a[iv.status] += secs
                for d, ds, de in split_by_midnight(s, e):
                    part = int((de - ds).total_seconds())
                    daily_c.append((version_id, d, resp.carrier_party, iv.flow_id,
                                    iv.status, part, part * qty))

        # 写 mart_quality_flow
        rows = []
        for fid, agg in flow_agg.items():
            qty = flow_qty[fid]
            rows.append((version_id, flow_lot[fid], fid, qty,
                         agg["normal"], agg["excursion"], agg["conflict"], agg["unknown"],
                         agg["normal"] * qty, agg["excursion"] * qty,
                         agg["conflict"] * qty, agg["unknown"] * qty))
        self._insert_many(
            "INSERT INTO mart_quality_flow VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        # 写 mart_quality_lot：受影响 lot 用其【全部】支流重算，避免局部重建丢失兄弟支流
        # 引擎结果包含全量支流区间，这里按 lot 汇总所有属于受影响 lot 的支流
        affected_lot_ids = {flow_lot[fid] for fid in do_flows} if not wide else {
            l.lot_id for l in lots}
        parent_ids = {f.parent_id for f in flows if f.parent_id}
        lot_agg = {}
        lot_secs = {}
        # 以全量支流区间为基准，只输出受影响 lot；父流拆分后不与子流同时计数
        full_flow_agg = {}
        for iv in result.intervals:
            agg = full_flow_agg.setdefault(iv.flow_id, {s: 0 for s in STATUSES})
            agg[iv.status] += int((iv.seg_end - iv.seg_start).total_seconds())
        for fid, agg in full_flow_agg.items():
            lid = flow_lot[fid]
            if lid not in affected_lot_ids:
                continue
            if fid in parent_ids:
                continue  # 父流盒数已由子流承载，lot 口径只累计叶子支流
            qty = flow_qty[fid]
            a = lot_agg.setdefault(lid, {s: 0 for s in STATUSES})
            b = lot_secs.setdefault(lid, {s: 0 for s in STATUSES})
            for st in STATUSES:
                a[st] += agg[st] * qty
                b[st] += agg[st]
        rows = []
        for lid, a in lot_agg.items():
            b = lot_secs[lid]
            rows.append((version_id, lid,
                         a["normal"], a["excursion"], a["conflict"], a["unknown"],
                         b["normal"], b["excursion"], b["conflict"], b["unknown"]))
        self._insert_many(
            "INSERT INTO mart_quality_lot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows,
        )
        # 承运方集市
        rows = []
        for party, a in sorted(carrier_agg.items()):
            rows.append((version_id, party,
                         a["normal"], a["excursion"], a["conflict"], a["unknown"],
                         a["normal"], a["excursion"], a["conflict"], a["unknown"]))
        self._insert_many(
            "INSERT INTO mart_carrier VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows,
        )
        # 日集市
        self._insert_many(
            "INSERT INTO mart_daily_quality VALUES (?, ?, ?, ?, ?, ?, ?)", daily_q,
        )
        self._insert_many(
            "INSERT INTO mart_daily_carrier VALUES (?, ?, ?, ?, ?, ?, ?)", daily_c,
        )
        # 可用量：仅叶子支流；窗口内全部 normal 才有可用量（按支流逐行，未受影响行已复制）
        rows = []
        for fid, agg in flow_agg.items():
            if fid in parent_ids:  # 父流拆分后不与子流同时计数
                continue
            avail = flow_qty[fid] if (
                agg["excursion"] == 0 and agg["conflict"] == 0 and agg["unknown"] == 0
            ) else 0
            rows.append((version_id, flow_lot[fid], fid, flow_qty[fid], avail,
                         agg["unknown"], agg["excursion"], agg["conflict"]))
        self._insert_many(
            "INSERT INTO mart_available VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows,
        )

    @staticmethod
    def flows_all(flows) -> list[str]:
        return [f.flow_id for f in flows]

    # ============================================================ 版本与复核

    def current_version(self) -> int | None:
        row = self.con.execute(
            "SELECT value FROM wh_meta WHERE key='current_version'").fetchone()
        return int(row[0]) if row else None

    def list_versions(self) -> list[dict]:
        cols = [d[0] for d in self.con.execute("SELECT * FROM versions LIMIT 0").description]
        return [dict(zip(cols, r)) for r in self.con.execute(
            "SELECT * FROM versions ORDER BY version_id").fetchall()]

    def save_review_ref(self, subject_type: str, subject_id: str,
                        note: str | None = None) -> str:
        version = self.current_version()
        ref_id = "R-" + uuid.uuid4().hex[:10]
        self.con.execute(
            "INSERT INTO review_refs VALUES (?, ?, ?, ?, ?, ?)",
            [ref_id, version, subject_type, subject_id, _now(), note],
        )
        return ref_id

    def get_review_ref(self, ref_id: str) -> dict:
        row = self.con.execute(
            "SELECT * FROM review_refs WHERE ref_id = ?", [ref_id]).fetchone()
        if not row:
            raise KeyError(f"复核引用不存在：{ref_id}")
        cols = [d[0] for d in self.con.execute(
            "SELECT * FROM review_refs LIMIT 0").description]
        return dict(zip(cols, row))

    def list_review_refs(self) -> list[dict]:
        cols = [d[0] for d in self.con.execute(
            "SELECT * FROM review_refs LIMIT 0").description]
        return [dict(zip(cols, r)) for r in self.con.execute(
            "SELECT * FROM review_refs ORDER BY created_at").fetchall()]

    # ============================================================ 查询（只读、钉版）

    def _resolve_version(self, version_id: int | None) -> int:
        if version_id is None:
            v = self.current_version()
            if v is None:
                raise ValueError("仓库尚无版本")
            return v
        return version_id

    def _query(self, sql: str, params: list, version_id: int | None):
        """在只读事务内查询：快照隔离，更新期间读到一致的同一版事实。"""
        v = self._resolve_version(version_id)
        self.con.execute("BEGIN TRANSACTION READ ONLY")
        try:
            cur = self.con.execute(sql, [v, *params])
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            self.con.execute("ROLLBACK")
        return rows

    def query_handoffs(self, flow_id=None, transfer_id=None, version_id=None):
        sql = "SELECT * FROM fact_handoffs WHERE version_id = ?"
        params = []
        if flow_id:
            sql += " AND flow_id = ?"
            params.append(flow_id)
        if transfer_id:
            sql += " AND transfer_id = ?"
            params.append(transfer_id)
        sql += " ORDER BY flow_id, COALESCE(sent_at, effective_received_at)"
        return self._query(sql, params, version_id)

    def query_intervals(self, flow_id, version_id=None):
        return self._query(
            "SELECT * FROM fact_temp_intervals WHERE version_id = ? AND flow_id = ? "
            "ORDER BY seg_start", [flow_id], version_id,
        )

    def query_gaps(self, flow_id, version_id=None):
        """未知区间及当前解释（没交接/没装载/没读数/无规则）。"""
        return self._query(
            "SELECT flow_id, seg_start, seg_end, status, unknown_reason, loggers, evidence_json "
            "FROM fact_temp_intervals WHERE version_id = ? AND flow_id = ? AND status='unknown' "
            "ORDER BY seg_start", [flow_id], version_id,
        )

    def query_conflicts(self, flow_id=None, version_id=None):
        sql = "SELECT * FROM fact_conflicts WHERE version_id = ?"
        params = []
        if flow_id:
            sql += " AND flow_id = ?"
            params.append(flow_id)
        return self._query(sql + " ORDER BY conflict_start", params, version_id)

    def query_quality_lot(self, lot_id=None, version_id=None):
        sql = "SELECT * FROM mart_quality_lot WHERE version_id = ?"
        params = []
        if lot_id:
            sql += " AND lot_id = ?"
            params.append(lot_id)
        return self._query(sql + " ORDER BY lot_id", params, version_id)

    def query_quality_flow(self, flow_id=None, version_id=None):
        sql = "SELECT * FROM mart_quality_flow WHERE version_id = ?"
        params = []
        if flow_id:
            sql += " AND flow_id = ?"
            params.append(flow_id)
        return self._query(sql + " ORDER BY flow_id", params, version_id)

    def query_carrier(self, party=None, version_id=None):
        sql = "SELECT * FROM mart_carrier WHERE version_id = ?"
        params = []
        if party:
            sql += " AND carrier_party = ?"
            params.append(party)
        return self._query(sql + " ORDER BY carrier_party", params, version_id)

    def query_daily_quality(self, day=None, lot_id=None, version_id=None):
        sql = "SELECT * FROM mart_daily_quality WHERE version_id = ?"
        params = []
        if day:
            sql += " AND day = ?"
            params.append(day)
        if lot_id:
            sql += " AND lot_id = ?"
            params.append(lot_id)
        return self._query(sql + " ORDER BY day, flow_id, status", params, version_id)

    def query_daily_carrier(self, day=None, party=None, version_id=None):
        sql = "SELECT * FROM mart_daily_carrier WHERE version_id = ?"
        params = []
        if day:
            sql += " AND day = ?"
            params.append(day)
        if party:
            sql += " AND carrier_party = ?"
            params.append(party)
        return self._query(sql + " ORDER BY day, carrier_party, flow_id", params, version_id)

    def query_available(self, lot_id=None, version_id=None):
        sql = "SELECT * FROM mart_available WHERE version_id = ?"
        params = []
        if lot_id:
            sql += " AND lot_id = ?"
            params.append(lot_id)
        return self._query(sql + " ORDER BY lot_id, flow_id", params, version_id)

    def query_responsibility(self, party=None, flow_id=None, version_id=None):
        sql = "SELECT * FROM fact_responsibility WHERE version_id = ?"
        params = []
        if party:
            sql += " AND carrier_party = ?"
            params.append(party)
        if flow_id:
            sql += " AND flow_id = ?"
            params.append(flow_id)
        return self._query(sql + " ORDER BY flow_id, seg_start", params, version_id)

    def review_snapshot(self, ref_id: str) -> dict:
        """按复核引用同时返回：当时事实/缺口与当前版本对应内容。"""
        ref = self.get_review_ref(ref_id)
        pinned_v, cur_v = ref["version_id"], self.current_version()
        stype, sid = ref["subject_type"], ref["subject_id"]
        if stype == "flow":
            pinned = {
                "intervals": self.query_intervals(sid, pinned_v),
                "gaps": self.query_gaps(sid, pinned_v),
                "handoffs": self.query_handoffs(flow_id=sid, version_id=pinned_v),
            }
            current = {
                "intervals": self.query_intervals(sid, cur_v),
                "gaps": self.query_gaps(sid, cur_v),
                "handoffs": self.query_handoffs(flow_id=sid, version_id=cur_v),
            } if cur_v != pinned_v else "与钉版相同"
        elif stype == "lot":
            pinned = {"quality": self.query_quality_lot(sid, pinned_v),
                      "available": self.query_available(sid, pinned_v)}
            current = ({"quality": self.query_quality_lot(sid, cur_v),
                        "available": self.query_available(sid, cur_v)}
                       if cur_v != pinned_v else "与钉版相同")
        elif stype == "carrier":
            pinned = {"carrier": self.query_carrier(sid, pinned_v)}
            current = ({"carrier": self.query_carrier(sid, cur_v)}
                       if cur_v != pinned_v else "与钉版相同")
        else:  # transfer
            pinned = {"handoffs": self.query_handoffs(transfer_id=sid, version_id=pinned_v)}
            current = ({"handoffs": self.query_handoffs(transfer_id=sid, version_id=cur_v)}
                       if cur_v != pinned_v else "与钉版相同")
        return {"ref": ref, "pinned_version": pinned_v,
                "current_version": cur_v, "pinned": pinned, "current": current}

    # ============================================================ 接收审计

    def ingest_report(self) -> list[dict]:
        self.con.execute("BEGIN TRANSACTION READ ONLY")
        try:
            cur = self.con.execute(
                "SELECT file_id, batch_id, table_name, file_name, sha256, "
                "byte_size, archived_at, row_count, outcome "
                "FROM ingest_files ORDER BY batch_id, table_name")
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            self.con.execute("ROLLBACK")
        return rows

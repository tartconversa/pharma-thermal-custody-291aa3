"""数据仓集成测试：入仓、版本化、补正、复核引用与故障恢复。"""
from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from coldchain.warehouse import Warehouse

RECEIVED_BASE = "2026-10-17T18:00:00+08:00"
TABLES = ["lots", "flows", "handoffs", "logger_loads",
          "readings", "temperature_rules"]


class WarehouseIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.db = self.tmp / "cc.duckdb"
        self.base = self.tmp / "base"
        self.base.mkdir()
        for name in TABLES:
            shutil.copy(Path(__file__).resolve().parents[1] / "data" / f"{name}.csv",
                        self.base / f"{name}.csv")
        self.wh = Warehouse(self.db)

    def tearDown(self):
        self.wh.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _ingest_base(self):
        return self.wh.ingest_pack(self.base, RECEIVED_BASE, "base")

    def test_initial_ingest_creates_version_and_persists_files(self):
        res = self._ingest_base()
        self.assertEqual(res["version_id"], 1)
        self.assertTrue(res["full_rebuild"])
        # 原始文件与接收情况持久保存
        report = self.wh.ingest_report()
        self.assertEqual(len(report), 6)
        self.assertTrue(all(r["outcome"] == "applied" for r in report))
        # 原始行留存
        n = self.wh.con.execute("SELECT count(*) FROM raw_rows").fetchone()[0]
        self.assertEqual(n, 77)

    def test_duplicate_pack_no_new_version(self):
        self._ingest_base()
        again = self.tmp / "again"
        again.mkdir()
        shutil.copy(self.base / "lots.csv", again / "lots.csv")
        res = self.wh.ingest_pack(again, "2026-10-18T08:00:00+08:00")
        self.assertEqual(res["version_id"], 1)  # 仍是同一版本
        self.assertIn("重复", res["reason"])
        # 接收历史仍记录了这次重复文件
        self.assertEqual(
            self.wh.con.execute(
                "SELECT count(*) FROM ingest_files WHERE outcome='duplicate'").fetchone()[0],
            1)

    def test_crash_resume_no_double_count(self):
        with self.assertRaises(RuntimeError):
            self.wh.ingest_pack(self.base, RECEIVED_BASE, fail_after_files=2)
        # 回滚干净
        self.assertIsNone(self.wh.current_version())
        self.assertEqual(
            self.wh.con.execute("SELECT count(*) FROM ingest_batches").fetchone()[0], 0)
        # 重跑成功
        res = self._ingest_base()
        self.assertEqual(res["version_id"], 1)
        self.assertEqual(
            self.wh.con.execute("SELECT count(*) FROM ingest_files").fetchone()[0], 6)
        # 区间时长与盒数不多算
        rows = self.wh.query_quality_flow()
        f1a = next(r for r in rows if r["flow_id"] == "F1A")
        self.assertEqual(f1a["conflict_seconds"], 5400)
        self.assertEqual(f1a["excursion_seconds"], 8100)

    def test_correction_scoped_rebuild_and_time_travel(self):
        self._ingest_base()
        corr_dir = self.tmp / "corr"
        corr_dir.mkdir()
        shutil.copy(Path(__file__).resolve().parents[1] / "data" / "time_corrections.csv",
                    corr_dir / "time_corrections.csv")
        res = self.wh.ingest_pack(corr_dir, "2026-10-18T08:00:00+08:00", "C1")
        self.assertEqual(res["version_id"], 2)
        self.assertEqual(res["affected_flows"], ["F1A"])  # 只重建受影响支流
        # v2 接管时间改到 16:30
        h2 = self.wh.query_handoffs(transfer_id="T2")[0]
        self.assertEqual(h2["effective_received_at"].minute, 30)
        # v1 仍是 17:00（时间旅行）
        h1 = self.wh.query_handoffs(transfer_id="T2", version_id=1)[0]
        self.assertEqual(h1["effective_received_at"].minute, 0)
        # 未受影响支流在 v2 仍可查（复制）
        self.assertTrue(self.wh.query_quality_flow(flow_id="F1B"))

    def test_review_ref_pins_version(self):
        self._ingest_base()
        ref = self.wh.save_review_ref("flow", "F1A", "复核")
        pinned = self.wh.review_snapshot(ref)
        self.assertEqual(pinned["pinned_version"], 1)
        # 再入补正产生新版本
        corr_dir = self.tmp / "corr"
        corr_dir.mkdir()
        shutil.copy(Path(__file__).resolve().parents[1] / "data" / "time_corrections.csv",
                    corr_dir / "time_corrections.csv")
        self.wh.ingest_pack(corr_dir, "2026-10-18T08:00:00+08:00")
        snap = self.wh.review_snapshot(ref)
        self.assertEqual(snap["pinned_version"], 1)
        self.assertEqual(snap["current_version"], 2)
        self.assertNotEqual(snap["current"], "与钉版相同")
        # 钉版内容仍指向旧的 17:00
        self.assertEqual(
            snap["pinned"]["handoffs"][0]["effective_received_at"].minute, 0)

    def test_unknown_reasons_distinguished(self):
        self._ingest_base()
        gaps = self.wh.query_gaps("F1A")
        reasons = {g["unknown_reason"] for g in gaps}
        self.assertIn("no_handoff", reasons)
        self.assertIn("no_reading", reasons)
        # F1A 的 no_handoff 段在接管之前
        nh = [g for g in gaps if g["unknown_reason"] == "no_handoff"]
        self.assertTrue(nh)

    def test_quantity_mismatch_and_revision(self):
        self._ingest_base()
        h = self.wh.query_handoffs(transfer_id="T3")[0]
        self.assertEqual(h["state"], "quantity_mismatch")
        self.assertEqual(h["matched_qty"], 58)
        self.assertEqual(h["disputed_qty"], 2)
        # 修订 H6 为 60
        rev = self.tmp / "rev"
        rev.mkdir()
        (rev / "handoffs.csv").write_text(
            "receipt_id,transfer_id,flow_id,side,party,counterparty,quantity,occurred_at\n"
            "H6,T3,F2,received,air,store,60,2026-10-16T09:00:00+08:00\n",
            encoding="utf-8")
        self.wh.ingest_pack(rev, "2026-10-19T10:00:00+08:00", "fix H6")
        h2 = self.wh.query_handoffs(transfer_id="T3")[0]
        self.assertEqual(h2["state"], "closed")
        self.assertEqual(h2["disputed_qty"], 0)
        # 原始历史保留两条 H6
        n = self.wh.con.execute(
            "SELECT count(*) FROM raw_rows WHERE record_key='H6'").fetchone()[0]
        self.assertEqual(n, 2)

    def test_no_double_count_overlapping_excursion(self):
        """同一时刻多仪器超限，盒·秒按时间并集，不翻倍。"""
        self._ingest_base()
        # F1A 20:00-22:00 有 G2/G3，冲突段只算一次 5400 秒
        f1a = next(r for r in self.wh.query_quality_flow() if r["flow_id"] == "F1A")
        # 手算预期：冲突 5400 秒，不会因两台仪器变 10800
        self.assertEqual(f1a["conflict_seconds"], 5400)

    def test_daily_mart_splits_midnight(self):
        """跨午夜片段按 +08:00 切到两个业务日。"""
        self._ingest_base()
        d16 = self.wh.query_daily_quality(day="2026-10-16")
        d17 = self.wh.query_daily_quality(day="2026-10-17")
        self.assertTrue(d16)
        self.assertTrue(d17)
        # F1B 窗口跨午夜，两天都应有记录
        self.assertTrue(any(r["flow_id"] == "F1B" for r in d16))
        self.assertTrue(any(r["flow_id"] == "F1B" for r in d17))

    def test_lot_mart_complete_after_scoped_rebuild(self):
        """局部重建只动 F1A，但 L1 批次集市仍须汇总全部叶子支流、不含父流。"""
        self._ingest_base()
        corr_dir = self.tmp / "corr"
        corr_dir.mkdir()
        shutil.copy(Path(__file__).resolve().parents[1] / "data" / "time_corrections.csv",
                    corr_dir / "time_corrections.csv")
        self.wh.ingest_pack(corr_dir, "2026-10-18T08:00:00+08:00")
        lot = next(r for r in self.wh.query_quality_lot(lot_id="L1"))
        leaf = {r["flow_id"]: r for r in self.wh.query_quality_flow()
                if r["flow_id"] in ("F1A", "F1B")}
        for st in ("normal", "excursion", "conflict", "unknown"):
            self.assertEqual(
                lot[f"{st}_box_seconds"],
                sum(r[f"{st}_box_seconds"] for r in leaf.values()))
        # 父流 F1 不参与 lot 汇总（拆分后不与子流重复计数）
        avail_flows = {r["flow_id"] for r in self.wh.query_available(lot_id="L1")}
        self.assertNotIn("F1", avail_flows)

    def test_available_excludes_unknown_and_excursion(self):
        self._ingest_base()
        avail = self.wh.query_available()
        # 所有叶子支流都含未知/超限/冲突 -> 无可用量（不替代放行）
        self.assertTrue(all(r["available_quantity"] == 0 for r in avail))
        # 父流 F1 拆分后不在可用量表（不与子流重复计数）
        self.assertFalse(any(r["flow_id"] == "F1" for r in avail))


if __name__ == "__main__":
    unittest.main()

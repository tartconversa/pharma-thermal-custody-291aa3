"""冷链口径引擎的规则级单测（纯逻辑，不碰数据库）。"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from coldchain.engine import (
    Correction,
    Engine,
    Flow,
    Load,
    Lot,
    Reading,
    Receipt,
    Rule,
)
from coldchain.times import f_to_c

TZ = timezone(timedelta(hours=8))


def dt(h: int, m: int = 0, day: int = 16) -> datetime:
    return datetime(2026, 10, day, h, m, tzinfo=TZ)


def make_engine(readings=None, loads=None, receipts=None, flows=None,
                corrections=None, rules=None, lots=None):
    lots = lots or [Lot("L1", "SKU1", 100)]
    flows = flows or [Flow("F1", "L1", None, 100, None)]
    rules = rules or [Rule("TR1", "SKU1", dt(0), dt(23, day=30), 2, 8)]
    return Engine(lots, flows, receipts or [], loads or [], readings or [],
                  rules, corrections or [])


class UnitAndReadingTests(unittest.TestCase):
    def test_fahrenheit_conversion(self):
        self.assertAlmostEqual(f_to_c(41), 5.0, places=6)
        self.assertAlmostEqual(f_to_c(32), 0.0, places=6)

    def test_reading_cover_end(self):
        r = Reading("R", "G", dt(8), 5, "C", 45)
        self.assertEqual(r.cover_end, dt(8, 45))

    def test_last_reading_not_extended(self):
        """末读到覆盖终点即止，之后为 no_reading，不无限延伸。"""
        readings = [Reading("R0", "G", dt(8), 5, "C", 45)]
        loads = [Load("A", "G", "F1", dt(8), dt(10))]
        eng = make_engine(readings, loads)
        res = eng.run()
        statuses = [(i.seg_start, i.status, i.unknown_reason) for i in res.intervals]
        # 08:45-10:00 无读数
        self.assertIn((dt(8, 45), "unknown", "no_reading"),
                      [(s, st, rs) for s, st, rs in statuses])

    def test_inclusive_bounds(self):
        """上下界含端点：2 和 8 都判正常。"""
        readings = [
            Reading("R1", "G", dt(8), 2, "C", 30),
            Reading("R2", "G", dt(9), 8, "C", 30),
            Reading("R3", "G", dt(10), 8.1, "C", 30),
        ]
        loads = [Load("A", "G", "F1", dt(8), dt(11))]
        eng = make_engine(readings, loads)
        verdicts = {i.seg_start: i.status for i in eng.run().intervals}
        self.assertEqual(verdicts[dt(8)], "normal")
        self.assertEqual(verdicts[dt(9)], "normal")
        self.assertEqual(verdicts[dt(10)], "excursion")


class EvidenceTests(unittest.TestCase):
    def _loads_readings_conflict(self):
        loads = [
            Load("A1", "G1", "F1", dt(8), dt(12)),
            Load("A2", "G2", "F1", dt(8), dt(12)),
        ]
        readings = []
        for h in range(8, 12):
            readings.append(Reading(f"G1-{h}", "G1", dt(h), 10, "C", 60))
            readings.append(Reading(f"G2-{h}", "G2", dt(h), 5, "C", 60))
        return loads, readings

    def test_overlapping_conflict(self):
        loads, readings = self._loads_readings_conflict()
        eng = make_engine(readings, loads)
        res = eng.run()
        self.assertTrue(all(i.status == "conflict" for i in res.intervals))
        self.assertEqual(len(res.conflicts), 1)  # 相邻冲突合并为一条范围
        self.assertEqual(res.conflicts[0].conflict_start, dt(8))
        self.assertEqual(res.conflicts[0].conflict_end, dt(12))

    def test_dual_excursion_union_not_double(self):
        """两台仪器都超限：按时间并集，区间只有一份，不按仪器数翻倍。"""
        loads = [
            Load("A1", "G1", "F1", dt(8), dt(10)),
            Load("A2", "G2", "F1", dt(8), dt(10)),
        ]
        readings = [
            Reading("G1-8", "G1", dt(8), 10, "C", 120),
            Reading("G2-8", "G2", dt(8), 11, "C", 120),
        ]
        eng = make_engine(readings, loads)
        intervals = eng.run().intervals
        excursion = [i for i in intervals if i.status == "excursion"]
        self.assertEqual(len(excursion), 1)  # 并集一条
        self.assertEqual(excursion[0].seg_end - excursion[0].seg_start,
                         timedelta(hours=2))

    def test_load_removal_stops_evidence(self):
        """仪器被移走后，其读数不能再证明该货物 -> no_load。"""
        receipts = [
            Receipt("H1", "T1", "F1", "sent", "store", "air", 100, dt(8)),
            Receipt("H2", "T1", "F1", "received", "air", "store", 100, dt(9)),
            Receipt("H3", "T2", "F1", "sent", "air", "cs", 100, dt(11)),
        ]
        loads = [Load("A", "G", "F1", dt(8), dt(9))]
        readings = [Reading(f"R{h}", "G", dt(h), 5, "C", 60)
                    for h in range(8, 12)]
        eng = make_engine(readings, loads, receipts)
        res = eng.run()
        after = [i for i in res.intervals if i.seg_start >= dt(9)]
        self.assertTrue(after)
        self.assertTrue(all(i.status == "unknown" and i.unknown_reason == "no_load"
                            for i in after))

    def test_child_does_not_inherit_load(self):
        """子流不自动继承未声明的仪器装载。"""
        flows = [
            Flow("F1", "L1", None, 100, dt(10)),
            Flow("F1A", "L1", "F1", 60, dt(10)),
            Flow("F1B", "L1", "F1", 40, dt(10)),
        ]
        receipts = [
            Receipt("H1", "T1", "F1", "sent", "store", "air", 100, dt(8)),
            Receipt("H2", "T1", "F1", "received", "air", "store", 100, dt(9)),
            Receipt("H3", "T2", "F1B", "sent", "air", "cs", 40, dt(11)),
        ]
        # 装载只声明在 F1，未声明给子流
        loads = [Load("A", "G", "F1", dt(8), dt(12))]
        readings = [Reading(f"R{h}", "G", dt(h), 5, "C", 60)
                    for h in range(8, 12)]
        eng = make_engine(readings, loads, receipts, flows)
        res = eng.run()
        f1b = [i for i in res.intervals if i.flow_id == "F1B"]
        self.assertTrue(f1b)
        self.assertTrue(all(i.unknown_reason == "no_load" for i in f1b))


class UnknownReasonTests(unittest.TestCase):
    def test_no_handoff_vs_no_load(self):
        receipts = [
            Receipt("H1", "T1", "F1", "sent", "store", "air", 100, dt(8)),
            Receipt("H2", "T1", "F1", "received", "air", "store", 100, dt(9)),
        ]
        # 装载 09:00-11:00；读数每小时一条、每条只覆盖 30 分钟，
        # 因此装载内每小时的后半小时有装载但无读数 -> no_reading
        loads = [Load("A", "G", "F1", dt(9), dt(11))]
        readings = [Reading(f"R{h}", "G", dt(h), 5, "C", 30)
                    for h in range(9, 11)]
        eng = make_engine(readings, loads, receipts)
        res = eng.run()
        gap_8_9 = [i for i in res.intervals
                   if dt(8) <= i.seg_start < dt(9) and i.unknown_reason == "no_handoff"]
        self.assertTrue(gap_8_9)  # 08-09 在途且无装载
        loaded_gap = [i for i in res.intervals if i.unknown_reason == "no_reading"]
        self.assertTrue(loaded_gap)  # 装载内读数之间的空档

    def test_no_rule(self):
        rules = [Rule("TR1", "SKU1", dt(12), dt(23, day=30), 2, 8)]
        loads = [Load("A", "G", "F1", dt(8), dt(11))]
        readings = [Reading(f"R{h}", "G", dt(h), 5, "C", 60)
                    for h in range(8, 11)]
        eng = make_engine(readings, loads, rules=rules)
        res = eng.run()
        self.assertTrue(all(i.unknown_reason == "no_rule" for i in res.intervals))


class HandoffTests(unittest.TestCase):
    def _two_receipts(self, sq, rq):
        return [
            Receipt("Hs", "T1", "F1", "sent", "store", "air", sq, dt(8)),
            Receipt("Hr", "T1", "F1", "received", "air", "store", rq, dt(9)),
        ]

    def test_closed(self):
        eng = make_engine(receipts=self._two_receipts(100, 100))
        h = eng.run().handoffs[0]
        self.assertEqual(h.state, "closed")
        self.assertEqual(h.matched_qty, 100)
        self.assertEqual(h.disputed_qty, 0)

    def test_quantity_mismatch_keeps_both(self):
        """数量差异不任取小值：相符与争议分别可查。"""
        eng = make_engine(receipts=self._two_receipts(60, 58))
        h = eng.run().handoffs[0]
        self.assertEqual(h.state, "quantity_mismatch")
        self.assertEqual(h.matched_qty, 58)
        self.assertEqual(h.disputed_qty, 2)

    def test_sent_only(self):
        eng = make_engine(receipts=[
            Receipt("Hs", "T1", "F1", "sent", "store", "air", 60, dt(8))])
        h = eng.run().handoffs[0]
        self.assertEqual(h.state, "sent_only")
        self.assertIsNone(h.received_receipt)


class CorrectionTests(unittest.TestCase):
    def test_pending_when_contradicts_split(self):
        flows = [
            Flow("F1", "L1", None, 100, dt(16)),
            Flow("F1A", "L1", "F1", 40, dt(16)),
        ]
        receipts = [
            Receipt("Hs", "T2", "F1A", "sent", "air", "road", 40, dt(16)),
            Receipt("Hr", "T2", "F1A", "received", "road", "air", 40, dt(17)),
        ]
        corrections = [Correction("C1", "Hs", dt(15), dt(18))]
        eng = make_engine(receipts=receipts, flows=flows, corrections=corrections)
        h = eng.run().handoffs[0]
        self.assertTrue(h.pending_review)
        self.assertEqual(h.sent_at, dt(16))  # 沿用原时点，不偷偷移动

    def test_valid_correction_moves_received(self):
        receipts = [
            Receipt("Hs", "T1", "F1", "sent", "store", "air", 100, dt(8)),
            Receipt("Hr", "T1", "F1", "received", "air", "store", 100, dt(9)),
        ]
        corrections = [Correction("C1", "Hr", dt(9, 30), dt(12))]
        eng = make_engine(receipts=receipts, corrections=corrections)
        h = eng.run().handoffs[0]
        self.assertFalse(h.pending_review)
        self.assertEqual(h.effective_received_at, dt(9, 30))

    def test_separate_corrections_per_side(self):
        receipts = [
            Receipt("Hs", "T1", "F1", "sent", "store", "air", 100, dt(8)),
            Receipt("Hr", "T1", "F1", "received", "air", "store", 100, dt(9)),
        ]
        corrections = [
            Correction("Cs", "Hs", dt(8, 30), dt(12)),
            Correction("Cr", "Hr", dt(9, 30), dt(12)),
        ]
        eng = make_engine(receipts=receipts, corrections=corrections)
        h = eng.run().handoffs[0]
        self.assertEqual(h.sent_correction_id, "Cs")
        self.assertEqual(h.received_correction_id, "Cr")
        self.assertEqual(h.sent_at, dt(8, 30))
        self.assertEqual(h.effective_received_at, dt(9, 30))


class SplitAndResponsibilityTests(unittest.TestCase):
    def test_parent_child_windows_disjoint(self):
        flows = [
            Flow("F1", "L1", None, 100, dt(16)),
            Flow("F1A", "L1", "F1", 40, dt(16)),
        ]
        receipts = [
            Receipt("H1", "T1", "F1", "sent", "store", "air", 100, dt(8)),
            Receipt("H2", "T1", "F1", "received", "air", "store", 100, dt(9)),
        ]
        eng = make_engine(receipts=receipts, flows=flows)
        windows = eng.run().windows
        self.assertEqual(windows["F1"][1], dt(16))   # 根流止于拆分
        # 子流无任何事件时不凭空生成窗口；有装载时才从拆分点起
        self.assertNotIn("F1A", windows)

    def test_repack_no_new_goods_quantity_conserved(self):
        """重新装箱不产生新货物：lot 盒数在支流间守恒。"""
        flows = [
            Flow("F1", "L1", None, 100, dt(16)),
            Flow("F1A", "L1", "F1", 40, dt(16)),
            Flow("F1B", "L1", "F1", 60, dt(16)),
        ]
        receipts = [
            Receipt("H1", "T1", "F1", "sent", "store", "air", 100, dt(8)),
            Receipt("H2", "T1", "F1", "received", "air", "store", 100, dt(9)),
        ]
        children = [f for f in flows if f.parent_id]
        self.assertEqual(sum(f.quantity for f in children), 100)
        # 子流有装载才形成窗口，且起点不早于拆分，父子窗口不重叠
        loads = [Load("A", "G", "F1A", dt(16), dt(18))]
        readings = [Reading("R1", "G", dt(16), 5, "C", 120)]
        eng = make_engine(readings, loads, receipts, flows)
        windows = eng.run().windows
        self.assertEqual(windows["F1"][1], dt(16))
        self.assertEqual(windows["F1A"][0], dt(16))
        self.assertLessEqual(windows["F1"][1], windows["F1A"][0])

    def test_responsibility_only_confirmed(self):
        """责任仅按已确认接管区间；在途空档不归属任何承运方。"""
        receipts = [
            Receipt("Hs", "T1", "F1", "sent", "store", "air", 100, dt(8)),
            Receipt("Hr", "T1", "F1", "received", "air", "store", 100, dt(9)),
        ]
        loads = [Load("A", "G", "F1", dt(8), dt(10))]
        readings = [Reading(f"R{h}", "G", dt(h), 5, "C", 60)
                    for h in range(8, 10)]
        eng = make_engine(readings, loads, receipts)
        res = eng.run()
        resp = res.responsibility
        # 责任从 receive 09:00 开始，不含 08-09 在途
        self.assertTrue(all(r.seg_start >= dt(9) for r in resp))
        holders = {r.carrier_party for r in resp}
        self.assertIn("air", holders)


if __name__ == "__main__":
    unittest.main()

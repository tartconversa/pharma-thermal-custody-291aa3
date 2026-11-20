"""冷链口径纯引擎。

输入为解析后的领域对象（dataclass），输出事实对象；不读写数据库、不碰文件，
所有业务规则集中在本模块，可独立单测。

核心规则（依据 docs/资料口径.md）：
- 读数从 at 起仅覆盖 coverage_minutes，并被装载区间截断；末读不延长。
- 仪器只在有装载关系的时段为对应支流提供证据；子流不继承未声明装载。
- 多仪器同区间判定矛盾 -> conflict；均超限按时间并集，不按仪器数累加。
- 未知区分：没交接(no_handoff) / 没装载(no_load) / 没读数(no_reading) / 无规则(no_rule)。
- 质量沿物理支流累计；责任仅按已确认接管区间；两者来自同一份区间事实。
- 补正不移动拆分实物时点；与拆分矛盾 -> 待核(pending_review)，沿用原时点。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .times import f_to_c

# ---------------------------------------------------------------- 领域对象


@dataclass(frozen=True)
class Lot:
    lot_id: str
    sku: str
    quantity: int


@dataclass(frozen=True)
class Flow:
    flow_id: str
    lot_id: str
    parent_id: str | None
    quantity: int
    split_at: datetime | None


@dataclass(frozen=True)
class Receipt:
    receipt_id: str
    transfer_id: str
    flow_id: str
    side: str
    party: str
    counterparty: str
    quantity: int
    occurred_at: datetime


@dataclass(frozen=True)
class Load:
    load_id: str
    logger_id: str
    flow_id: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class Reading:
    reading_id: str
    logger_id: str
    at: datetime
    value: float
    unit: str
    coverage_minutes: int

    @property
    def cover_end(self) -> datetime:
        return self.at + timedelta(minutes=self.coverage_minutes)

    @property
    def value_c(self) -> float:
        return self.value if self.unit == "C" else f_to_c(self.value)


@dataclass(frozen=True)
class Rule:
    rule_id: str
    sku: str
    start: datetime
    end: datetime
    lower_c: float
    upper_c: float


@dataclass(frozen=True)
class Correction:
    correction_id: str
    receipt_id: str
    new_at: datetime
    received_at: datetime


# ---------------------------------------------------------------- 事实对象


@dataclass
class HandoffFact:
    transfer_id: str
    flow_id: str
    sent_receipt: str | None
    received_receipt: str | None
    sender: str | None
    receiver: str | None
    sent_at: datetime | None
    received_at: datetime | None
    effective_received_at: datetime | None
    sent_qty: int | None
    received_qty: int | None
    matched_qty: int
    disputed_qty: int
    state: str
    correction_id: str | None
    sent_correction_id: str | None
    received_correction_id: str | None
    pending_review: bool
    pending_reason: str | None


@dataclass
class OwnershipSegment:
    flow_id: str
    seg_start: datetime
    seg_end: datetime
    holder: str | None
    basis: str


@dataclass
class ResponsibilitySegment:
    flow_id: str
    carrier_party: str
    seg_start: datetime
    seg_end: datetime
    transfer_id: str | None
    end_pending: bool


@dataclass
class TempInterval:
    flow_id: str
    lot_id: str
    sku: str
    seg_start: datetime
    seg_end: datetime
    status: str
    unknown_reason: str | None
    loggers: list[str]
    evidence: list[dict]


@dataclass
class ConflictRecord:
    flow_id: str
    conflict_start: datetime
    conflict_end: datetime
    loggers: list[str]
    detail: list[dict]


@dataclass
class EngineResult:
    handoffs: list[HandoffFact]
    ownership: list[OwnershipSegment]
    responsibility: list[ResponsibilitySegment]
    intervals: list[TempInterval]
    conflicts: list[ConflictRecord]
    windows: dict[str, tuple[datetime, datetime]]


# ---------------------------------------------------------------- 引擎


class Engine:
    def __init__(self, lots, flows, receipts, loads, readings, rules, corrections):
        self.lots = {l.lot_id: l for l in lots}
        self.flows = {f.flow_id: f for f in flows}
        self.receipts = receipts
        self.loads = loads
        self.readings_by_logger: dict[str, list[Reading]] = {}
        for r in readings:
            self.readings_by_logger.setdefault(r.logger_id, []).append(r)
        for lst in self.readings_by_logger.values():
            lst.sort(key=lambda x: x.at)
        self.rules = rules
        self.corrections = sorted(corrections, key=lambda c: c.received_at)
        self.children: dict[str, list[Flow]] = {}
        for f in flows:
            if f.parent_id:
                self.children.setdefault(f.parent_id, []).append(f)
        for lst in self.children.values():
            lst.sort(key=lambda x: (x.split_at, x.flow_id))

    # ------------------------------------------------ 补正与交接事实

    def _correction_for(self, receipt_id: str):
        """按接收次序取最后一条指向该回执的补正。"""
        hit = None
        for c in self.corrections:
            if c.receipt_id == receipt_id:
                hit = c
        return hit

    def _check_pending(self, receipt: Receipt, new_at: datetime) -> str | None:
        """补正时间与拆分实物时点矛盾时返回待核原因，否则 None。"""
        flow = self.flows.get(receipt.flow_id)
        if flow is None or flow.split_at is None:
            return None
        is_child = flow.parent_id not in (None, "")
        if is_child and new_at < flow.split_at:
            return f"补正时点 {new_at.isoformat()} 早于支流拆分 {flow.split_at.isoformat()}"
        if not is_child and new_at >= flow.split_at:
            return f"补正时点 {new_at.isoformat()} 不早于根流拆分 {flow.split_at.isoformat()}"
        return None

    def _apply_correction(self, receipt: Receipt | None, effective: datetime | None):
        """对单张回执应用其补正，返回 (新时点, 补正id, 待核原因)。"""
        if receipt is None:
            return effective, None, None
        corr = self._correction_for(receipt.receipt_id)
        if corr is None:
            return effective, None, None
        reason = self._check_pending(receipt, corr.new_at)
        if reason:
            return effective, corr.correction_id, reason  # 待核：沿用原时点
        return corr.new_at, corr.correction_id, None

    def build_handoffs(self) -> list[HandoffFact]:
        groups: dict[tuple[str, str], dict[str, Receipt]] = {}
        for r in self.receipts:
            groups.setdefault((r.transfer_id, r.flow_id), {})[r.side] = r
        facts: list[HandoffFact] = []
        for (transfer_id, flow_id), pair in sorted(groups.items()):
            s = pair.get("sent")
            r = pair.get("received")
            sent_at = s.occurred_at if s else None
            received_at = r.occurred_at if r else None
            # 两张回执的补正分别处理，互不覆盖
            sent_at, sent_corr, sent_pending = self._apply_correction(s, sent_at)
            effective_received, recv_corr, recv_pending = self._apply_correction(
                r, received_at)
            pending_reason = sent_pending or recv_pending
            pending = pending_reason is not None
            correction_id = recv_corr or sent_corr  # 兼容单列：优先接管侧补正
            sent_qty = s.quantity if s else None
            recv_qty = r.quantity if r else None
            if s and r:
                state = "closed" if sent_qty == recv_qty else "quantity_mismatch"
                matched = min(sent_qty, recv_qty)
                disputed = abs(sent_qty - recv_qty)
            else:
                state = "sent_only" if s else "received_only"
                matched = 0
                disputed = sent_qty if s else recv_qty
            facts.append(
                HandoffFact(
                    transfer_id, flow_id,
                    s.receipt_id if s else None,
                    r.receipt_id if r else None,
                    s.party if s else None,
                    r.party if r else None,
                    sent_at, received_at, effective_received,
                    sent_qty, recv_qty, matched, disputed,
                    state, correction_id, sent_corr, recv_corr,
                    pending, pending_reason,
                )
            )
        facts.sort(key=lambda h: (h.flow_id, h.sent_at or h.effective_received_at or datetime.max))
        return facts

    # ------------------------------------------------ 支流窗口

    def flow_windows(self, handoffs: list[HandoffFact]) -> dict[str, tuple[datetime, datetime]]:
        """支流有效窗口：根流止于拆分；子流起于拆分。左闭右开，互不重叠。

        子流即使没有自己的交接/装载，也从拆分点起持有货物，窗口至少覆盖拆分点；
        根流没有任何交接则不形成可观察窗口。
        """
        windows: dict[str, tuple[datetime, datetime]] = {}
        for flow in self.flows.values():
            starts, ends = [], []
            is_child = flow.parent_id not in (None, "")
            if flow.split_at is not None:
                if is_child:  # 子流：起于拆分，保证无事件也有窗口
                    starts.append(flow.split_at)
                    ends.append(flow.split_at)
                else:  # 根流：止于拆分
                    ends.append(flow.split_at)
            for h in handoffs:
                if h.flow_id != flow.flow_id:
                    continue
                for t in (h.sent_at, h.effective_received_at):
                    if t is None:
                        continue
                    if flow.split_at is not None and is_child and t < flow.split_at:
                        continue
                    if flow.split_at is not None and not is_child and t >= flow.split_at:
                        continue
                    starts.append(t)
                    ends.append(t)
            for ld in self.loads:
                if ld.flow_id != flow.flow_id:
                    continue
                starts.append(ld.start)
                ends.append(ld.end)
            if not starts or not ends:
                continue  # 根流没有任何业务时间，不形成窗口
            w_start, w_end = min(starts), max(ends)
            if w_start < w_end:  # 跳过零长度窗口（仅有拆分点、无任何经历）
                windows[flow.flow_id] = (w_start, w_end)
        return windows

    # ------------------------------------------------ 归属与责任

    def build_ownership(self, handoffs, windows) -> list[OwnershipSegment]:
        out: list[OwnershipSegment] = []
        ordered = self._flows_topological()
        owner_by_flow: dict[str, list[OwnershipSegment]] = {}
        # 根流最后持有方：子流在拆分点继承（父流窗口恰在拆分点结束时回退使用）
        root_last_holder: dict[str, str | None] = {}
        for flow in ordered:
            fid = flow.flow_id
            if fid not in windows:
                continue
            w_start, w_end = windows[fid]
            events = []  # (time, kind, holder, basis)
            for h in handoffs:
                if h.flow_id != fid:
                    continue
                if h.sent_at is not None:
                    events.append((h.sent_at, 1, None, "sent"))
                if h.effective_received_at is not None:
                    events.append((h.effective_received_at, 0, h.receiver, "received"))
            events.sort(key=lambda e: (e[0], e[1]))  # 同刻 receive 先生效
            # 初始持有方：子流继承父流拆分时点持有方；根流取首个发出方
            if flow.parent_id:
                holder = self._holder_at(owner_by_flow.get(flow.parent_id, []), w_start)
                if holder is None:
                    holder = root_last_holder.get(flow.parent_id)
            else:
                first_sender = next((h.sender for h in handoffs
                                     if h.flow_id == fid and h.sender), None)
                holder = first_sender
            segs: list[OwnershipSegment] = []
            cur, mark = holder, "initial"
            cursor = w_start
            for t, _kind, new_holder, basis in events:
                if t <= cursor or t >= w_end:
                    if w_start <= t < w_end:
                        cur, mark = new_holder, basis
                    continue
                segs.append(OwnershipSegment(fid, cursor, t, cur, mark))
                cur, mark = new_holder, basis
                cursor = t
            if cursor < w_end:
                segs.append(OwnershipSegment(fid, cursor, w_end, cur, "flow_end"))
            owner_by_flow[fid] = segs
            if not flow.parent_id:
                root_last_holder[fid] = cur
            out.extend(segs)
        return out

    def _flows_topological(self) -> list[Flow]:
        roots = [f for f in self.flows.values() if not f.parent_id]
        out: list[Flow] = []

        def walk(f: Flow):
            out.append(f)
            for child in self.children.get(f.flow_id, []):
                walk(child)

        for r in sorted(roots, key=lambda f: f.flow_id):
            walk(r)
        return out

    @staticmethod
    def _holder_at(segments: list[OwnershipSegment], t: datetime):
        for seg in segments:
            if seg.seg_start <= t < seg.seg_end:
                return seg.holder
        return None

    def build_responsibility(self, handoffs, ownership, windows) -> list[ResponsibilitySegment]:
        """责任仅按已确认接管区间：持有方非空的归属段；尾部无人接管标 end_pending。"""
        # 支流最后一个事件是否为只有发出（缺 receive）-> 责任终点待核
        pending_flows = set()
        transfer_by_flow: dict[str, list[HandoffFact]] = {}
        for h in handoffs:
            transfer_by_flow.setdefault(h.flow_id, []).append(h)
        for fid, hs in transfer_by_flow.items():
            hs2 = sorted(hs, key=lambda x: x.sent_at or x.effective_received_at)
            last = hs2[-1]
            if last.state == "sent_only":
                pending_flows.add(fid)
        # 每段归属对应的交接：找段起点最近的 received/sent 事件
        out: list[ResponsibilitySegment] = []
        for seg in ownership:
            if seg.holder is None:
                continue  # 在途空档不是任何承运方的责任
            fid = seg.flow_id
            transfer_id = None
            for h in transfer_by_flow.get(fid, []):
                if h.effective_received_at == seg.seg_start or h.sent_at == seg.seg_start:
                    transfer_id = h.transfer_id
            w_end = windows[fid][1]
            end_pending = fid in pending_flows and seg.seg_end == w_end
            out.append(
                ResponsibilitySegment(
                    fid, seg.holder, seg.seg_start, seg.seg_end,
                    transfer_id, end_pending,
                )
            )
        out.sort(key=lambda x: (x.flow_id, x.seg_start))
        return out

    # ------------------------------------------------ 温控区间

    def _rule_at(self, sku: str, t: datetime) -> Rule | None:
        for rule in self.rules:
            if rule.sku == sku and rule.start <= t < rule.end:
                return rule
        return None

    def build_intervals(self, ownership, windows) -> tuple[list[TempInterval], list[ConflictRecord]]:
        intervals: list[TempInterval] = []
        conflicts: list[ConflictRecord] = []
        owner_index = {fid: segs for fid, segs in self._group_ownership(ownership).items()}
        for flow in self._flows_topological():
            fid = flow.flow_id
            if fid not in windows:
                continue
            w_start, w_end = windows[fid]
            lot = self.lots[flow.lot_id]
            sku = lot.sku
            flow_loads = [ld for ld in self.loads if ld.flow_id == fid]
            # 该流装载区间（与窗口相交）
            load_parts = []
            for ld in flow_loads:
                s, e = max(ld.start, w_start), min(ld.end, w_end)
                if s < e:
                    load_parts.append((s, e, ld))
            # 边界集合
            bounds = {w_start, w_end}
            for s, e, _ld in load_parts:
                bounds.add(s)
                bounds.add(e)
            # 读数覆盖段（经装载截断）
            cover_parts = []  # (s,e,load,reading|None)
            for s, e, ld in load_parts:
                bounds.add(s)
                bounds.add(e)
                readings = self.readings_by_logger.get(ld.logger_id, [])
                for rd in readings:
                    cs, ce = max(rd.at, s), min(rd.cover_end, e)
                    if cs < ce:
                        cover_parts.append((cs, ce, ld, rd))
                        bounds.add(cs)
                        bounds.add(ce)
            # 规则边界
            for rule in self.rules:
                if rule.sku != sku:
                    continue
                rs, re_ = max(rule.start, w_start), min(rule.end, w_end)
                if rs < re_:
                    bounds.add(rs)
                    bounds.add(re_)
            timeline = sorted(bounds)
            for s, e in zip(timeline, timeline[1:]):
                if s < w_start or e > w_end or s >= e:
                    continue
                holder = self._holder_at(owner_index.get(fid, []), s)
                rule = self._rule_at(sku, s)  # 规则对段内所有仪器一致
                covering = [
                    (ls, le, ld) for ls, le, ld in load_parts if ls <= s and e <= le
                ]
                evidence = []
                verdicts = []  # (logger, verdict)
                any_rule = rule is not None
                for _ls, _le, ld in covering:
                    rd = self._reading_covering(ld.logger_id, s, e)
                    item = {
                        "load_id": ld.load_id,
                        "logger_id": ld.logger_id,
                        "reading_id": rd.reading_id if rd else None,
                        "reading_at": rd.at.isoformat() if rd else None,
                        "value_c": rd.value_c if rd else None,
                        "unit": rd.unit if rd else None,
                        "verdict": None,
                    }
                    if rd is not None and rule is not None:
                        verdict = "normal" if rule.lower_c <= rd.value_c <= rule.upper_c else "excursion"
                        item["verdict"] = verdict
                        item["rule_id"] = rule.rule_id
                        verdicts.append((ld.logger_id, verdict))
                    evidence.append(item)
                loggers = [ld.logger_id for _ls, _le, ld in covering]
                if not covering:
                    status, reason = "unknown", ("no_handoff" if holder is None else "no_load")
                elif not verdicts:
                    status = "unknown"
                    reason = "no_rule" if not any_rule else "no_reading"
                else:
                    kinds = {v for _lg, v in verdicts}
                    reason = None
                    if len(kinds) == 2:
                        status = "conflict"
                    else:
                        status = next(iter(kinds))
                intervals.append(
                    TempInterval(fid, flow.lot_id, sku, s, e,
                                 status, reason, loggers, evidence)
                )
            # 合并相邻冲突段
            conflicts.extend(self._merge_conflicts(fid, intervals))
        return intervals, conflicts

    @staticmethod
    def _group_ownership(ownership: list[OwnershipSegment]):
        idx: dict[str, list[OwnershipSegment]] = {}
        for seg in ownership:
            idx.setdefault(seg.flow_id, []).append(seg)
        for segs in idx.values():
            segs.sort(key=lambda x: x.seg_start)
        return idx

    def _reading_covering(self, logger_id: str, s: datetime, e: datetime):
        """取覆盖原子段起点 s 的读数（原子段由覆盖边界切出，段内判定恒定）。"""
        for rd in self.readings_by_logger.get(logger_id, []):
            if rd.at <= s < rd.cover_end:
                return rd
        return None

    @staticmethod
    def _merge_conflicts(fid: str, intervals: list[TempInterval]) -> list[ConflictRecord]:
        out: list[ConflictRecord] = []
        cur = None
        for iv in intervals:
            if iv.flow_id != fid or iv.status != "conflict":
                if cur:
                    out.append(cur)
                    cur = None
                continue
            if cur and cur.conflict_end == iv.seg_start:
                cur.conflict_end = iv.seg_end
                for ev in iv.evidence:
                    cur.detail.append({"at": iv.seg_start.isoformat(), **ev})
            else:
                if cur:
                    out.append(cur)
                cur = ConflictRecord(
                    fid, iv.seg_start, iv.seg_end, list(iv.loggers),
                    [{"at": iv.seg_start.isoformat(), **ev} for ev in iv.evidence],
                )
        if cur:
            out.append(cur)
        return out

    # ------------------------------------------------ 总入口

    def run(self) -> EngineResult:
        handoffs = self.build_handoffs()
        windows = self.flow_windows(handoffs)
        ownership = self.build_ownership(handoffs, windows)
        responsibility = self.build_responsibility(handoffs, ownership, windows)
        intervals, conflicts = self.build_intervals(ownership, windows)
        return EngineResult(handoffs, ownership, responsibility, intervals, conflicts, windows)

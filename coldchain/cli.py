"""冷链仓命令行入口。

用法示例：
  python -I -m coldchain.cli init --db warehouse/cc.duckdb
  python -I -m coldchain.cli ingest --db warehouse/cc.duckdb --pack packs/base --received-at 2026-10-17T18:00:00+08:00
  python -I -m coldchain.cli ingest --db warehouse/cc.duckdb --pack packs/correction --received-at 2026-10-18T08:00:00+08:00
  python -I -m coldchain.cli query ... / review ... / refs ...
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

if __package__ in (None, ""):  # 允许 python3 -I coldchain/cli.py 直接运行
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from coldchain.times import iso8
    from coldchain.warehouse import Warehouse
else:
    from .times import iso8
    from .warehouse import Warehouse


def _json_default(value):
    if isinstance(value, datetime):
        return iso8(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _print(data, as_json: bool):
    if as_json:
        print(json.dumps(data, ensure_ascii=False, indent=2, default=_json_default))
        return
    if isinstance(data, list):
        if not data:
            print("（无记录）")
            return
        headers = list(data[0].keys())
        widths = {h: max(len(h), *(len(str(r.get(h, ""))) for r in data)) for h in headers}
        print("  ".join(h.ljust(widths[h]) for h in headers))
        print("  ".join("-" * widths[h] for h in headers))
        for r in data:
            print("  ".join(str(r.get(h, "")).ljust(widths[h]) for h in headers))
    else:
        print(json.dumps(data, ensure_ascii=False, indent=2, default=_json_default))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="coldchain", description="药品冷链温控经历仓")
    p.add_argument("--db", default="warehouse/cc.duckdb", help="DuckDB 仓库文件")
    p.add_argument("--json", action="store_true", help="JSON 输出")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="初始化仓库")

    ing = sub.add_parser("ingest", help="接收一个资料包目录")
    ing.add_argument("--pack", required=True)
    ing.add_argument("--received-at", required=True, help="本次接收时刻（ISO，带时区）")
    ing.add_argument("--note")
    ing.add_argument("--fail-after-files", type=int, default=None,
                     help="故障注入：处理 N 个有效文件后中断（测试用）")

    sub.add_parser("versions", help="列出全部版本")
    sub.add_parser("files", help="原始文件接收与去重情况")

    q = sub.add_parser("query", help="查询事实与集市")
    q.add_argument("subject",
                   choices=["handoffs", "intervals", "gaps", "conflicts",
                            "quality-lot", "quality-flow", "carrier",
                            "daily-quality", "daily-carrier", "available",
                            "responsibility"])
    q.add_argument("--flow")
    q.add_argument("--lot")
    q.add_argument("--transfer")
    q.add_argument("--party")
    q.add_argument("--day")
    q.add_argument("--version", type=int)

    rv = sub.add_parser("review", help="保存或读取复核引用（钉版）")
    rv.add_argument("action", choices=["save", "show", "list"])
    rv.add_argument("--ref")
    rv.add_argument("--subject-type", choices=["flow", "lot", "carrier", "transfer"])
    rv.add_argument("--subject-id")
    rv.add_argument("--note")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    wh = Warehouse(args.db)
    try:
        if args.command == "init":
            _print({"status": "initialized", "db": args.db,
                    "current_version": wh.current_version()}, args.json)
        elif args.command == "ingest":
            res = wh.ingest_pack(
                args.pack, args.received_at, args.note,
                fail_after_files=args.fail_after_files,
            )
            _print(res, args.json)
        elif args.command == "versions":
            _print(wh.list_versions(), args.json)
        elif args.command == "files":
            _print(wh.ingest_report(), args.json)
        elif args.command == "query":
            v = args.version
            s = args.subject
            if s == "handoffs":
                out = wh.query_handoffs(flow_id=args.flow, transfer_id=args.transfer, version_id=v)
            elif s == "intervals":
                out = wh.query_intervals(args.flow, v)
            elif s == "gaps":
                out = wh.query_gaps(args.flow, v)
            elif s == "conflicts":
                out = wh.query_conflicts(flow_id=args.flow, version_id=v)
            elif s == "quality-lot":
                out = wh.query_quality_lot(lot_id=args.lot, version_id=v)
            elif s == "quality-flow":
                out = wh.query_quality_flow(flow_id=args.flow, version_id=v)
            elif s == "carrier":
                out = wh.query_carrier(party=args.party, version_id=v)
            elif s == "daily-quality":
                out = wh.query_daily_quality(day=args.day, lot_id=args.lot, version_id=v)
            elif s == "daily-carrier":
                out = wh.query_daily_carrier(day=args.day, party=args.party, version_id=v)
            elif s == "available":
                out = wh.query_available(lot_id=args.lot, version_id=v)
            else:
                out = wh.query_responsibility(party=args.party, flow_id=args.flow, version_id=v)
            _print(out, args.json)
        elif args.command == "review":
            if args.action == "save":
                ref = wh.save_review_ref(args.subject_type, args.subject_id, args.note)
                _print({"ref_id": ref, "version_id": wh.current_version()}, args.json)
            elif args.action == "list":
                _print(wh.list_review_refs(), args.json)
            else:
                _print(wh.review_snapshot(args.ref), args.json)
    finally:
        wh.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .ops_snapshot_v1 import render_v1, validate_repo_v1


def main() -> int:
    p = argparse.ArgumentParser(prog="srcf-sync")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("render-v1", help="Render a v1 snapshot file (RFC8785 canonical JSON)")
    r.add_argument("--out-root", default=".", help="Repo root to write into")
    r.add_argument("--table", required=True, help="Table slug (^[a-z0-9][a-z0-9-]*$)")
    r.add_argument("--row-id", required=True, help="Upstream row id (string)")
    r.add_argument("--record-json", required=True, help="Path to upstream row JSON file")
    r.add_argument("--system", default="baserow", help="Upstream system identifier")

    v = sub.add_parser("validate-v1", help="Validate snapshots/v1 against contract")
    v.add_argument("--out-root", default=".", help="Repo root to validate")

    args = p.parse_args()

    if args.cmd == "render-v1":
        record = json.loads(Path(args.record_json).read_text(encoding="utf-8"))
        out = render_v1(
            out_root=Path(args.out_root),
            table=args.table,
            row_id=str(args.row_id),
            record=record,
            system=args.system,
        )
        print(str(out))
        return 0

    if args.cmd == "validate-v1":
        errors = validate_repo_v1(out_root=Path(args.out_root))
        if errors:
            for e in errors:
                print(e)
            return 1
        return 0

    return 2

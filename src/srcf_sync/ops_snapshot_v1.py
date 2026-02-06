from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import rfc8785

SCHEMA_V1 = "srcf.ops.snapshot.v1"
CIVIL_TZ = "Europe/London"
TABLE_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")


def _now_rfc3339_z() -> str:
    # RFC3339 format with explicit UTC "Z" suffix
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _validate_row_id(row_id: str) -> None:
    if row_id in (".", ".."):
        raise ValueError(f"row_id must not be {row_id!r}")
    if "/" in row_id or "\\" in row_id:
        raise ValueError("row_id must not contain path separators")


def snapshot_path(out_root: Path, table: str, row_id: str) -> Path:
    # Contract: snapshots/v1/<table>/<row_id>.json
    return out_root / "snapshots" / "v1" / table / f"{row_id}.json"


def render_v1(*, out_root: Path, table: str, row_id: str, record: Any, system: str = "baserow") -> Path:
    if not TABLE_RE.match(table):
        raise ValueError(f"invalid table slug {table!r}")
    _validate_row_id(row_id)

    # Contract requires record to be object; preserve upstream payload.
    if not isinstance(record, dict):
        record = {"_raw": record}

    obj: Dict[str, Any] = {
        "schema": SCHEMA_V1,
        "source": {
            "system": system,
            "table": table,
            "row_id": row_id,
            "generated_at": _now_rfc3339_z(),
            "civil_tz": CIVIL_TZ,
        },
        "record": record,
    }

    path = snapshot_path(out_root, table, row_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    # RFC8785: write canonical UTF-8 bytes
    path.write_bytes(rfc8785.dumps(obj))
    return path


def validate_repo_v1(*, out_root: Path) -> List[str]:
    errors: List[str] = []
    base = out_root / "snapshots" / "v1"
    if not base.exists():
        errors.append("missing snapshots/v1")
        return errors

    for p in base.rglob("*.json"):
        raw = p.read_bytes()

        # Must be UTF-8 JSON
        try:
            obj = json.loads(raw.decode("utf-8"))
        except Exception as e:
            errors.append(f"{p}: invalid UTF-8 JSON: {e}")
            continue

        # Must be JCS-canonical on disk (byte-for-byte)
        try:
            canon = rfc8785.dumps(obj)
        except Exception as e:
            errors.append(f"{p}: not RFC8785-canonicalizable: {e}")
            continue
        if raw != canon:
            errors.append(f"{p}: not RFC8785-canonical bytes (re-serialization differs)")
            continue

        if not isinstance(obj, dict):
            errors.append(f"{p}: top-level must be object")
            continue

        if obj.get("schema") != SCHEMA_V1:
            errors.append(f"{p}: schema must be {SCHEMA_V1!r}")
            continue

        source = obj.get("source")
        if not isinstance(source, dict):
            errors.append(f"{p}: source must be object")
            continue

        if source.get("civil_tz") != CIVIL_TZ:
            errors.append(f"{p}: source.civil_tz must be {CIVIL_TZ!r}")
            continue

        ga = source.get("generated_at")
        # Contract-lite: require RFC3339 with Z (simple structural check)
        if not (isinstance(ga, str) and ga.endswith("Z") and "T" in ga):
            errors.append(f"{p}: source.generated_at must be RFC3339 with 'Z'")
            continue

        table = source.get("table")
        row_id = source.get("row_id")
        if not isinstance(table, str) or not isinstance(row_id, str):
            errors.append(f"{p}: source.table and source.row_id must be strings")
            continue

        # Must match the canonical path
        expected = snapshot_path(out_root, table, row_id).resolve()
        if p.resolve() != expected:
            errors.append(f"{p}: path mismatch (expected {expected})")
            continue

        record = obj.get("record")
        if not isinstance(record, dict):
            errors.append(f"{p}: record must be object")
            continue

    return errors

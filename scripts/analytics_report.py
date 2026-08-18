"""Read-only summary of persisted PAPER strategy analytics."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

# Allow direct execution as ``python scripts/analytics_report.py``.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analytics import analyze


def _read_rows(database_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not database_path.exists():
        raise FileNotFoundError(database_path)
    resolved = database_path.resolve()
    uri = f"file:///{resolved.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        opportunities = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM opportunities ORDER BY timestamp"
            )
        ]
        trades = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM paper_trades ORDER BY entry_timestamp"
            )
        ]
        return opportunities, trades
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only PAPER analytics report")
    parser.add_argument(
        "--database",
        default="paper_trading.sqlite3",
        help="SQLite database path (default: paper_trading.sqlite3)",
    )
    args = parser.parse_args()
    opportunities, trades = _read_rows(Path(args.database))
    report = analyze(opportunities, trades).as_dict()
    print(json.dumps(report["strategy_analytics"], indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

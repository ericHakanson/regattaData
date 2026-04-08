#!/usr/bin/env python3
"""Generate structured address repair suggestions for overloaded line1 values."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from regatta_etl.direct_mail_review import repair_overloaded_line1_addresses


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create address repair suggestions when line1 appears overloaded "
            "with city/state/postal content."
        )
    )
    parser.add_argument("--input", required=True, help="Input reviewed CSV path")
    parser.add_argument("--output", required=True, help="Output repaired CSV path")
    parser.add_argument(
        "--summary-json",
        default=None,
        help="Optional summary JSON output path",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    summary = repair_overloaded_line1_addresses(
        input_path=Path(args.input),
        output_path=Path(args.output),
        summary_path=Path(args.summary_json) if args.summary_json else None,
    )
    print(json.dumps(summary.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Normalize reviewer-added direct-mail CSV columns to the contract schema."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from regatta_etl.direct_mail_review import normalize_reviewer_csv


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize direct-mail reviewer CSV columns (header aliases + enum "
            "and boolean values)."
        )
    )
    parser.add_argument("--input", required=True, help="Input reviewer CSV path")
    parser.add_argument("--output", required=True, help="Normalized output CSV path")
    parser.add_argument(
        "--summary-json",
        default=None,
        help="Optional summary JSON output path",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    summary = normalize_reviewer_csv(
        input_path=Path(args.input),
        output_path=Path(args.output),
        summary_path=Path(args.summary_json) if args.summary_json else None,
    )
    print(json.dumps(summary.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

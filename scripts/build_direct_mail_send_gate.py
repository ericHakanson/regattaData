#!/usr/bin/env python3
"""Build send-gated direct-mail CSV from reviewer annotations."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from regatta_etl.direct_mail_review import apply_send_safety_gate


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Apply direct-mail send safety gate from reviewer annotations and "
            "emit send_requested/send_ready fields."
        )
    )
    parser.add_argument("--input", required=True, help="Input reviewer CSV path")
    parser.add_argument("--output", required=True, help="Output gated CSV path")
    parser.add_argument(
        "--summary-json",
        default=None,
        help="Optional summary JSON output path",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    summary = apply_send_safety_gate(
        input_path=Path(args.input),
        output_path=Path(args.output),
        summary_path=Path(args.summary_json) if args.summary_json else None,
    )
    print(json.dumps(summary.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

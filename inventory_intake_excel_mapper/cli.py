import argparse
import sys
from pathlib import Path
from typing import Iterable, List

from .mapper import MapperConfig, load_sources, map_sources_to_target


GLOB_CHARS = set("*?[")


def _has_glob(text: str) -> bool:
    return any(char in text for char in GLOB_CHARS)


def _expand_paths(items: Iterable[str]) -> List[Path]:
    results: List[Path] = []
    for item in items:
        path = Path(item)
        if _has_glob(item):
            results.extend(sorted(Path().glob(item)))
        elif path.is_dir():
            results.extend(sorted(path.glob("*.xls*")))
        else:
            results.append(path)
    return [p for p in results if p.exists()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Map data from multiple source Excel files into target Excel templates."
    )
    parser.add_argument("--sources", nargs="+", required=True, help="Source Excel files or globs")
    parser.add_argument("--targets", nargs="+", required=True, help="Target Excel files or globs")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--inplace", action="store_true", help="Write changes into target files")
    parser.add_argument("--min-score", type=int, default=80, help="Minimum header match score")
    parser.add_argument("--fill", default="N/A", help="Fill value for blanks")
    parser.add_argument(
        "--drop-zero-rows",
        choices=["all", "any", "off"],
        default="all",
        help="Drop rows with zero values",
    )
    parser.add_argument("--key", help="Key column name to merge rows")
    parser.add_argument("--append", action="store_true", help="Append to existing target rows")
    parser.add_argument("--source-sheet", help="Source sheet name to read")
    parser.add_argument("--target-sheet", help="Target sheet name to write")
    parser.add_argument(
        "--header-scan-rows",
        type=int,
        default=20,
        help="Rows to scan when detecting the header",
    )
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    sources = _expand_paths(args.sources)
    targets = _expand_paths(args.targets)

    if not sources:
        print("No source files found.", file=sys.stderr)
        return 2
    if not targets:
        print("No target files found.", file=sys.stderr)
        return 2

    config = MapperConfig(
        min_score=args.min_score,
        fill_value=args.fill,
        drop_zero_mode=args.drop_zero_rows,
        append=args.append,
        key=args.key,
        source_sheet=args.source_sheet,
        target_sheet=args.target_sheet,
        header_scan_rows=args.header_scan_rows,
    )

    source_df = load_sources(sources, config.source_sheet)

    output_dir = Path(args.output_dir)
    if not args.inplace:
        output_dir.mkdir(parents=True, exist_ok=True)

    for target in targets:
        output_path = target if args.inplace else output_dir / target.name
        map_sources_to_target(source_df, target, output_path, config)

    print(f"Processed {len(targets)} target file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

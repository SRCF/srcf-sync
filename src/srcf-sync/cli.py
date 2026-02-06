import argparse

def main() -> int:
    parser = argparse.ArgumentParser(prog="srcf-sync")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("version", help="Print version")

    args = parser.parse_args()
    if args.cmd == "version":
        print("srcf-sync 0.1.0")
        return 0

    return 2

if __name__ == "__main__":
    raise SystemExit(main())

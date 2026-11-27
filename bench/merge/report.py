import json
import sys
from pathlib import Path

OUT = Path("target/bench-merge")


def fmt_secs(s: float) -> str:
    return f"{s * 1e3:.0f} ms" if s < 1 else f"{s:.2f} s"


def main() -> None:
    for name in sys.argv[1:] or ["B", "C", "D", "E"]:
        path = OUT / f"results-{name}.json"
        if not path.exists():
            continue
        rows = json.loads(path.read_text())
        print(f"\n## {name}\n")
        print("| case | client | time | CPU | rows | windows | rows/s | open | peak MiB | RSS MiB | ok |")
        print("|---|---|---|---|---|---|---|---|---|---|---|")
        for r in rows:
            if "error" in r:
                print(f"| {r['case']} | {r['client']} | error: {r['error'][:80]} |")
                continue
            ok = "yes" if r["same_rows"] and r["same_order"] else "NO"
            spread = max(r["secs_runs"]) - min(r["secs_runs"])
            print(
                f"| {r['case']} | {r['client']} | {fmt_secs(r['secs'])} ±{spread * 1e3:.0f} | {r['cpu']:.2f} | {r['rows']} | {r['windows']} | "
                f"{r['rows'] / r['secs'] / 1e3:.0f}k | {r['open_ms']:.0f} ms | {r['peak_mib']:.0f} | {r['rss_mib']:.0f} | {ok} |"
            )


if __name__ == "__main__":
    main()

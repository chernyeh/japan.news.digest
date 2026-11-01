"""
backfill_estimates_log.py — seed data/estimates_log/ from what the repo
already remembers.

The log starts recording on its first collector run; everything before that
exists only as past versions of files in git. This reads them once:

  * every committed version of data/consensus.csv — the street's numbers and
    company guidance as each collector run left them (not the reported
    figures, whose dates a snapshot cannot tell; the collector logs those from
    the filings);
  * every committed version of data/consensus_manual.json — typed and
    screenshot values, including ones later retyped or deleted;
  * data/guidance_history.csv — the first and latest guidance for each year,
    which reaches back further than either file's git history.

Safe to re-run: the log only keeps changes, so rows it already holds are
dropped on merge. Run from the repo root with full git history
(`git fetch --unshallow` on a shallow clone):

    python backfill_estimates_log.py
"""

import csv
import io
import json
import subprocess
import sys

import estimates_log as EL
import fundamentals as F


def _git(*args) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True,
                          text=True).stdout


def _versions(path: str) -> list:
    """[(sha, commit_date)] for every commit that touched `path`, oldest first."""
    out = _git("log", "--reverse", "--format=%H %ad", "--date=short", "--", path)
    return [tuple(line.split()) for line in out.splitlines() if line.strip()]


def _show(sha: str, path: str) -> str:
    try:
        return _git("show", f"{sha}:{path}")
    except subprocess.CalledProcessError:
        return ""


def main() -> int:
    if _git("rev-parse", "--is-shallow-repository").strip() == "true":
        print("Shallow clone — run `git fetch --unshallow` first.", file=sys.stderr)
        return 1
    rows = {}

    def add(code, new):
        rows.setdefault(code, []).extend(new)

    versions = _versions(F.CONSENSUS_PATH)
    print(f"{F.CONSENSUS_PATH}: {len(versions)} version(s)")
    for i, (sha, day) in enumerate(versions, 1):
        text = _show(sha, F.CONSENSUS_PATH)
        by_code = {}
        for r in csv.DictReader(io.StringIO(text)):
            by_code.setdefault(r.get("code", ""), []).append(r)
        for code, rs in by_code.items():
            if code:
                # Reported figures are skipped inside rows_from_consensus:
                # a snapshot cannot date them (see EL.REPORTED_BASES).
                add(code, EL.rows_from_consensus(rs, day, origin="backfill"))
        # Compact as we go: twenty versions of a 55,000-row file are mostly
        # the same rows, and holding them all would be most of a gigabyte.
        for code in by_code:
            if code in rows:
                rows[code] = EL.compact(rows[code])
        print(f"  [{i}/{len(versions)}] {day} {sha[:8]}")

    versions = _versions(F.MANUAL_PATH)
    print(f"{F.MANUAL_PATH}: {len(versions)} version(s)")
    for sha, day in versions:
        try:
            data = json.loads(_show(sha, F.MANUAL_PATH) or "{}")
        except ValueError:
            continue
        for code, entries in data.items():
            add(code, EL.rows_from_manual(entries, day))

    n_hist = 0
    for r in F.read_rows(F.GUIDANCE_HISTORY_PATH, F.GUIDANCE_HISTORY_COLUMNS):
        for val, when in ((r.get("first_value"), r.get("first_as_of")),
                          (r.get("latest_value"), r.get("latest_as_of"))):
            if val not in (None, "") and when:
                add(r["code"], [{"date": when[:10], "metric": r["metric"], "fy": r["fy"],
                                 "basis": "company", "value": val, "source": "jquants",
                                 "origin": "backfill", "logged_at": when[:10]}])
                n_hist += 1
    print(f"{F.GUIDANCE_HISTORY_PATH}: {n_hist} first/latest guidance point(s)")

    grew = sum(EL.write_local(code, rs) for code, rs in sorted(rows.items()))
    print(f"\nEstimates log: {grew} row(s) across {len(rows)} companies -> {EL.LOG_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Tests for results_notes.py — `python test_results_notes.py` (no network)."""

import results_notes as RN


def test_one_note_per_result_and_empty_is_none():
    e = RN.make_entry("8136", "FY2027", "3Q", " beat on OP\nguide kept ", "2026-11-06", "2026-11-06")
    assert e["id"] == "8136-FY2027-3Q" and e["text"] == "beat on OP\nguide kept"
    assert RN.make_entry("8136", "FY2027", "3Q", "  \n", "x", "x") is None


def test_apply_replaces_same_result_and_keeps_others():
    a = RN.make_entry("8136", "FY2027", "2Q", "first", "2026-08-07", "2026-08-07")
    b = RN.make_entry("8136", "FY2027", "3Q", "second", "2026-11-06", "2026-11-06")
    data = RN.apply({}, "8136", [a, b])
    b2 = dict(b, text="revised")
    data = RN.apply(data, "8136", [b2])
    assert [e["text"] for e in data["8136"]] == ["revised", "first"]
    data = RN.apply(data, "8136", remove_ids=[a["id"], b["id"]])
    assert "8136" not in data


def test_labels_round_trip_and_options():
    assert RN.parse_label(RN.label("FY2027", "FY")) == ("FY2027", "FY")
    assert RN.parse_label("junk") == ("", "")
    opts = RN.options("FY2027")
    assert opts[:4] == ["Full year FY2027", "3Q FY2027", "2Q FY2027", "1Q FY2027"]
    assert "1Q FY2026" in opts


if __name__ == "__main__":
    import sys
    fails = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn(); print(f"ok   {name}")
            except Exception as exc:
                fails += 1; print(f"FAIL {name}: {exc!r}")
    sys.exit(1 if fails else 0)

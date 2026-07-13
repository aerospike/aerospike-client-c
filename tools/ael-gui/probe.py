#!/usr/bin/env python3
"""Expression error-details invariant prober.

Drives the ael-gui server's /eval endpoint with AEL expressions and checks
structural invariants on the returned error details (field 45), so trace
bugs (wrong spans, missing operands, bad outcome classification, snippet
disagreement) surface mechanically instead of by eyeballing JSON.

Usage:
    ./probe.py                  # run the built-in battery
    ./probe.py -e '$.x == 999'  # probe one expression
    ./probe.py -v               # also print PASSing rows' traces
    ./probe.py -b http://127.0.0.1:8299   # non-default ael-gui port

Invariants checked per response (when applicable):
  I1  span-in-bounds: 0 <= ael_offset, ael_offset+ael_span <= len(src)
  I2  snippet-focus agreement: the text between >>..<< focus marks in the
      snippet equals the utf-8 slice src[ael_offset : ael_offset+ael_span]
      (modulo edge-truncation '...' windows)
  I3  depth == len(path)
  I4  path[-1] == op
  I5  lang == AEL(2) whenever a trace is present for AEL source
  I6  outcome matches the message family (false/absent/fault)
  I7  operand gating: FALSE-outcome comparisons carry operands (read ctx);
      or/exclusive/not/bin decisives don't
  I8  phase sanity: eval traces carry no msgpack byte_offset for AEL
  I9  utf-8 sanity: the span slice decodes cleanly (no split multibyte)
"""

import argparse
import json
import sys
import urllib.request

BASE = "http://127.0.0.1:8280"
MARK_L = "»"  # >>
MARK_R = "«"  # <<

COMPARISON_OPS = {"eq", "ne", "gt", "ge", "lt", "le"}
NO_OPERAND_OPS = {"or", "exclusive", "not", "bin", "and", "xor"}


def eval_expr(src, mode="filter", verbosity=3):
    req = urllib.request.Request(
        f"{BASE}/eval?mode={mode}&verbosity={verbosity}",
        data=src.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


def check(src, resp, expect=None):
    """Return list of (invariant, detail) violations."""
    flags = []
    det = resp.get("details") or {}
    t = det.get("trace") or {}
    msg = det.get("message") or ""
    sb = src.encode()

    off, span = t.get("ael_offset"), t.get("ael_span")
    if off is not None and span is not None:
        # I1 bounds
        if off < 0 or off + span > len(sb):
            flags.append(("I1", f"span [{off},{off}+{span}) outside 0..{len(sb)}"))
        else:
            # I9 utf-8 sanity
            try:
                focus = sb[off:off + span].decode()
            except UnicodeDecodeError:
                flags.append(("I9", f"span slice splits a multibyte char"))
                focus = sb[off:off + span].decode(errors="replace")
            # I2 snippet agreement
            snip = t.get("snippet")
            if snip is not None:
                if MARK_L in snip and MARK_R in snip:
                    marked = snip.split(MARK_L, 1)[1].rsplit(MARK_R, 1)[0]
                    m = marked.strip(".")  # edge-truncation windows
                    if marked != focus and not (m and m in focus):
                        flags.append(("I2", f"focus {marked!r} != span slice {focus!r}"))
                elif span > 0:
                    flags.append(("I2", f"span={span} but snippet has no focus marks: {snip!r}"))

    # I3/I4 path shape. A >16-deep chain truncates the path (keeps the
    # outermost frames + "..." + the target op) while depth stays the true
    # nesting depth - allow depth > len(path) iff the "..." marker is there.
    path, depth, op = t.get("path"), t.get("depth"), t.get("op")
    if path is not None and depth is not None and depth != len(path):
        if not ("..." in path and depth > len(path)):
            flags.append(("I3", f"depth {depth} != len(path) {len(path)}"))
    if path and op is not None and path[-1] != op:
        flags.append(("I4", f"path[-1] {path[-1]!r} != op {op!r}"))

    # I5 lang
    if t and t.get("lang") != 2:
        flags.append(("I5", f"lang {t.get('lang')} != 2 (AEL)"))

    # I6 outcome vs message
    oc = t.get("outcome")
    if oc is not None:
        fam = {"evaluated to false": 2, "absent bin or key": 3}
        for needle, want in fam.items():
            if needle in msg and oc != want:
                flags.append(("I6", f"msg {msg!r} but outcome {oc}"))
        if oc == 1 and "filtered out -" in msg:
            flags.append(("I6", f"fault outcome but filtered-out message {msg!r}"))

    # I7 operand gating (filter mode == read-authorized)
    operands = t.get("operands")
    if oc == 2 and op in COMPARISON_OPS and not operands:
        flags.append(("I7", f"FALSE {op} decisive but no operands"))
    if operands and op in NO_OPERAND_OPS:
        flags.append(("I7", f"op {op} should not carry operands: {operands}"))

    # I8 no msgpack byte_offset on AEL traces
    if t and t.get("offset") is not None:
        flags.append(("I8", f"AEL trace carries msgpack byte_offset {t.get('offset')}"))

    # caller expectations
    if expect:
        for k, want in expect.items():
            if k == "msg_contains":
                if want not in msg:
                    flags.append(("EXP", f"message {msg!r} lacks {want!r}"))
            elif k == "focus":
                snip = t.get("snippet") or ""
                marked = (snip.split(MARK_L, 1)[1].rsplit(MARK_R, 1)[0]
                          if MARK_L in snip else None)
                if marked != want:
                    flags.append(("EXP", f"focus {marked!r}, expected {want!r}"))
            elif k == "no_trace":
                if bool(t) == want:
                    flags.append(("EXP", f"trace present={bool(t)}, expected absent={want}"))
            else:
                got = t.get(k) if k in ("op", "outcome", "depth", "operands") \
                    else (resp.get(k) if k == "status_name" else det.get(k))
                if got != want:
                    flags.append(("EXP", f"{k}={got!r}, expected {want!r}"))

    return flags


def run(cases, verbose=False):
    n_flag = 0
    for case in cases:
        src, expect = (case if isinstance(case, tuple) else (case, None))
        try:
            resp = eval_expr(src)
        except Exception as e:
            print(f"ERR   {src!r}: {e}")
            n_flag += 1
            continue
        flags = check(src, resp, expect)
        t = (resp.get("details") or {}).get("trace") or {}
        line = (f"op={t.get('op','-'):<10} oc={t.get('outcome','-')!s:<2} "
                f"off={t.get('ael_offset','-')!s:>3} span={t.get('ael_span','-')!s:>3} "
                f"snip={t.get('snippet','-')!r}")
        if flags:
            n_flag += 1
            print(f"FLAG  {src!r}")
            print(f"      {line}")
            for inv, why in flags:
                print(f"      {inv}: {why}")
            print(f"      msg={(resp.get('details') or {}).get('message')!r} "
                  f"status={resp.get('status_name')}")
        elif verbose:
            print(f"ok    {src!r}")
            print(f"      {line}")
        else:
            print(f"ok    {src!r}")
    return n_flag


BATTERY = [
    # ---- n-ary chains (2026-07-06 ast_nary_merge re-stamp fix) ----
    ('$.x == 999 or $.y == 2.6 or $.name == "aed" or 1 == 2',
     {"op": "or", "outcome": 2,
      "focus": '$.x == 999 or $.y == 2.6 or $.name == "aed" or 1 == 2'}),
    ('$.x == 999 or ($.y == 2.6 or $.name == "aed")',
     {"op": "or", "focus": '$.x == 999 or ($.y == 2.6 or $.name == "aed")'}),
    # ---- decisive comparisons + operands ----
    ('$.name == "ael" and $.x == 11',
     {"op": "eq", "outcome": 2, "focus": "$.x == 11",
      "operands": ["10", "11"]}),
    ('$.name == "zzz" and ($.x == 11 or $.y == 2.6)',
     {"op": "eq", "focus": '$.name == "zzz"', "operands": ["ael", "zzz"]}),
    # ---- faults ----
    ('$.x / 0 == 1', {"op": "div", "outcome": 1,
                      "msg_contains": "division by zero"}),
    # true spans (server 97485408d+): the default arm's result spans exactly
    # its own construct - no more 'default =>' keyword-prefix inheritance
    # from the case boundary.
    ('when($.x == 999 => 1, default => 2 / 0) == 1',
     {"op": "div", "outcome": 1, "focus": "2 / 0"}),
    ('when($.x == 999 => 1, $.x / 0 == 1 => 2, default => 3) == 3',
     {"op": "div", "outcome": 1, "focus": "$.x / 0"}),
    ('when($.x == 999 => 1, $.x == 10 => 2 / 0, default => 3) == 1',
     {"op": "div", "outcome": 1, "focus": "2 / 0"}),
    # ---- absent (outcome 3) attribution ----
    ('$.missing == 2', {"op": "bin", "outcome": 3, "focus": "$.missing"}),
    ('$.x == 999 or $.missing == 1',
     {"op": "bin", "outcome": 3, "focus": "$.missing"}),
    ('$.missing == 1 and $.x == 10',
     {"op": "bin", "outcome": 3, "focus": "$.missing"}),
    ('exclusive($.x == 10, $.missing == 1)',
     {"op": "bin", "outcome": 3, "focus": "$.missing"}),
    ('$.key() == 1', {"op": "key", "outcome": 3, "focus": "$.key()"}),
    ('$.m.zzz:INT == 1', {"op": "call", "outcome": 3}),
    ('when($.missing == 1 => 1, default => 2) == 2',
     {"op": "bin", "outcome": 3}),
    # ---- wrappers pierce to the deciding child (rt_eval exclusion fix) ----
    ('let(a = $.x + 1) then (${a} == 99)',
     {"op": "eq", "outcome": 2, "operands": ["11", "99"]}),
    ('when($.x == 11 => 1, $.y == 9.9 => 2, default => 3) == 99',
     {"op": "eq", "outcome": 2, "operands": ["3", "99"]}),
    # ---- when/let construct spans (grammar re-stamp fix) ----
    ('let(a = $.x / 0) then (${a} == 1)',
     {"op": "div", "outcome": 1}),
    ('let(a = $.missing:INT) then (${a} == 1)',
     {"op": "bin", "outcome": 3}),
    # ---- CDT ----
    ('$.xs:LIST.[0]:INT == 99',
     {"op": "eq", "outcome": 2, "operands": ["1", "99"]}),
    ('$.m.a:INT == 99', {"op": "eq", "outcome": 2}),
    ('$.xs:LIST.count() == 99', {"op": "eq", "outcome": 2}),
    ('$.blob:BLOB.bitGetInt(offset: 0, size: 8) == 99',
     {"op": "eq", "outcome": 2}),
    # CDT-layer message + late-attached trace (2026-07-06 append fix): the
    # CDT op authors its message first, the boundary's trace appends after.
    ('$.xs:LIST.[9]:INT == 1',
     {"op": "call", "outcome": 1, "msg_contains": "out of bounds"}),
    ('$.xs:LIST.*[?(@ / 0 > 1)].count() == 1',
     {"op": "call", "outcome": 1, "msg_contains": "cdt select"}),
    # ---- non-comparison decisives: no operands ----
    ('not($.x == 10)', {"op": "not", "outcome": 2}),
    ('$.x in [1, 2, 3]', {"op": "in_list", "outcome": 2}),
    # ---- build failures ----
    ('$.x ==', {"status_name": "AEROSPIKE_ERR_REQUEST_INVALID",
                "msg_contains": "unexpected end of expression"}),
    # non-boolean filter: message-only build detail (check_filter_exp fix)
    ('$.x / 0', {"status_name": "AEROSPIKE_ERR_REQUEST_INVALID",
                 "msg_contains": "must evaluate to boolean"}),
    # ---- misc / edges ----
    ('$.name == "aél and ünïcode"', {"op": "eq", "outcome": 2}),
    ('$.y / 0.0 == 1.0', {"op": "eq", "outcome": 2}),  # inf, no float fault
    # metadata-phase FALSE: not explained (known gap - message only)
    ('$.ttl() == -5', {"no_trace": True}),
    # deep fault: path truncates with "...", depth stays true (I3 allowance)
    ('not(not(not(not(not(not(not(not(not(not(not(not(not(not(not(not(not('
     'not($.x / 0 == 1))))))))))))))))))',
     {"op": "div", "outcome": 1}),
]

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--expr", action="append")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("-b", "--base", default=BASE,
                    help="ael-gui base URL (default %(default)s)")
    args = ap.parse_args()
    BASE = args.base.rstrip("/")
    cases = args.expr if args.expr else BATTERY
    sys.exit(1 if run(cases, args.verbose) else 0)

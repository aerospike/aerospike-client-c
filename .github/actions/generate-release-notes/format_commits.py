#!/usr/bin/env python3
"""
format_commits.py — read git log subjects from stdin, emit markdown bullet lines.

Each line is formatted as:
    * <Description>. [TICKET]    (when a Jira ticket is found in the subject)
    * <Description>.             (no ticket)

Ticket patterns matched: CLIENT-NNNN, AER-NNNN, DOCS-NNNN
Leading ticket prefixes stripped: "CLIENT-1234: " / "CLIENT-1234 - " / "[CLIENT-1234] "
"""

import sys
import re

TICKET_RE = re.compile(r'\b(CLIENT|AER|DOCS)-[0-9]+\b')
PREFIX_RE = re.compile(r'^\[?(CLIENT|AER|DOCS)-[0-9]+\]?[: \-]+')

for raw in sys.stdin:
    subject = raw.strip()
    if not subject:
        continue

    m = TICKET_RE.search(subject)
    ticket = m.group(0) if m else ""

    desc = PREFIX_RE.sub("", subject).rstrip(".")
    desc = desc[0].upper() + desc[1:] if desc else desc

    if ticket:
        print(f"* {desc}. [{ticket}]")
    else:
        print(f"* {desc}.")

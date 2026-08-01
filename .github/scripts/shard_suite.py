#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Split the benchmark suite across shards, balancing by cost.

Each shard runs base and head for its own subset of entries. That keeps the comparison
valid: the A/B pair for any one benchmark still runs back to back on a single machine, and
results from different benchmarks are never compared against each other, so they have no
reason to share a runner.

Entry costs vary by three orders of magnitude, so splitting round-robin leaves one shard
holding most of the work. Entries may therefore carry a "# cost=<seconds>" hint, which this
script packs longest-first into the least-loaded shard (LPT). The hints only affect balance,
never results, so a stale hint costs a little wall clock and nothing else.
"""

import argparse
import re
import sys

# Entries without a hint are assumed mid-weight: heavy enough not to be dumped together into
# one shard, light enough not to distort packing if the real cost turns out to be small.
DEFAULT_COST = 10.0

COST_RE = re.compile(r"#\s*cost\s*=\s*([0-9.]+)")


def parse_suite(path):
    """Return [(entry_text_without_comment, cost)] for non-empty, non-comment lines."""
    entries = []
    with open(path) as f:
        for raw in f:
            cost_match = COST_RE.search(raw)
            body = raw.split("#", 1)[0].strip()
            if not body:
                continue
            cost = float(cost_match.group(1)) if cost_match else DEFAULT_COST
            entries.append((body, cost))
    return entries


def pack(entries, shards):
    """Greedy longest-processing-time-first bin packing."""
    bins = [[] for _ in range(shards)]
    loads = [0.0] * shards
    # Sort by descending cost, tie-broken by text so the split is deterministic across the
    # separate runner processes that each compute it independently.
    for body, cost in sorted(entries, key=lambda e: (-e[1], e[0])):
        target = loads.index(min(loads))
        bins[target].append(body)
        loads[target] += cost
    return bins, loads


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True)
    parser.add_argument("--shards", type=int, required=True)
    parser.add_argument("--index", type=int, required=True, help="1-based shard number")
    parser.add_argument("--summary", action="store_true", help="print the balance to stderr")
    args = parser.parse_args()

    if args.shards < 1 or not (1 <= args.index <= args.shards):
        raise SystemExit("shard index {} out of range for {} shards".format(args.index, args.shards))

    entries = parse_suite(args.suite)
    if not entries:
        raise SystemExit("No benchmark entries found in {}".format(args.suite))
    if len(entries) < args.shards:
        raise SystemExit(
            "{} entries cannot fill {} shards; reduce the shard count".format(
                len(entries), args.shards
            )
        )

    bins, loads = pack(entries, args.shards)

    if args.summary:
        for i, load in enumerate(loads, start=1):
            print(
                "shard {}/{}: {} entries, ~{:.0f}s".format(i, args.shards, len(bins[i - 1]), load),
                file=sys.stderr,
            )

    for body in bins[args.index - 1]:
        print(body)


if __name__ == "__main__":
    main()

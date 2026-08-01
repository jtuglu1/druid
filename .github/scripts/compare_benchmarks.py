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

"""Compare two sets of JMH JSON results and render a markdown report.

Reads the base-*.json / head-*.json files written by run_benchmarks.sh, pools the raw
per-iteration measurements for each benchmark across all rounds, and reports the ratio of
medians with a bootstrap confidence interval.

Pooling the raw iterations rather than trusting a single run's reported scoreError is what
makes short runs usable: two rounds of "-i 10" give 20 measurements per variant, which is
enough to resolve a shift of roughly 5-10% on a shared CI runner.

Standard library only, by design -- ubuntu-latest has no numpy or scipy, and a pip install
step inside a timing-sensitive job is worth avoiding.
"""

import argparse
import glob
import json
import os
import random
import statistics
import sys

# A benchmark must move by at least this much *and* have a CI excluding 1.0 before it is
# called out. Below this, shared-runner noise is a more likely explanation than the change.
MIN_EFFECT = 0.10
BOOTSTRAP_RESAMPLES = 2000
CONFIDENCE = 0.95
# A bootstrap over very few samples is not just imprecise, it is actively misleading: resampling
# a single measurement returns that same value every time, so the interval collapses to zero
# width and appears to exclude 1.0 with total confidence. Below this many pooled samples per
# side, report the measurement but issue no verdict.
MIN_SAMPLES = 8
# Fixed seed so re-running the comparator on the same JSON gives the same table.
SEED = 19740321


def load_results(results_dir, variant):
    """Pool raw iteration samples per benchmark key for one variant.

    Returns {key: {"samples": [...], "unit": str, "mode": str, "params": {...}}}.
    """
    pooled = {}
    toolchain = set()
    pattern = os.path.join(results_dir, "{}-*.json".format(variant))
    for path in sorted(glob.glob(pattern)):
        with open(path) as f:
            try:
                records = json.load(f)
            except json.JSONDecodeError as e:
                raise SystemExit("Malformed JMH output in {}: {}".format(path, e))
        for record in records:
            # JMH's blackhole mode and other measurement machinery depend on the JMH and
            # JVM versions, so a PR that changes either makes the two sides incomparable.
            toolchain.add(
                (record.get("jmhVersion", "?"), record.get("vmVersion", "?"))
            )
            params = record.get("params", {})
            key = (record["benchmark"], tuple(sorted(params.items())))
            metric = record["primaryMetric"]
            # rawData is a list (one entry per fork) of lists (one per iteration).
            samples = [v for fork in metric.get("rawData", []) for v in fork]
            if not samples:
                # Fall back to the summary score if a JMH version omits raw data.
                samples = [metric["score"]]
            entry = pooled.setdefault(
                key,
                {
                    "samples": [],
                    "unit": metric.get("scoreUnit", ""),
                    "mode": record.get("mode", ""),
                    "params": params,
                },
            )
            entry["samples"].extend(samples)
    return pooled, toolchain


def higher_is_better(mode, unit):
    """JMH throughput modes report ops/time, where a larger score is an improvement."""
    if mode:
        return mode.lower() in ("thrpt", "throughput")
    # scoreUnit for throughput looks like "ops/s"; for the time-based modes it is "s/op".
    return unit.startswith("ops/")


def bootstrap_ratio_ci(base, head, rng):
    """Percentile bootstrap CI for median(head) / median(base)."""
    ratios = []
    n_base, n_head = len(base), len(head)
    for _ in range(BOOTSTRAP_RESAMPLES):
        b = statistics.median(rng.choices(base, k=n_base))
        h = statistics.median(rng.choices(head, k=n_head))
        if b == 0:
            continue
        ratios.append(h / b)
    if not ratios:
        return (float("nan"), float("nan"))
    ratios.sort()
    lo_idx = int((1 - CONFIDENCE) / 2 * len(ratios))
    hi_idx = min(len(ratios) - 1, int((1 + CONFIDENCE) / 2 * len(ratios)))
    return (ratios[lo_idx], ratios[hi_idx])


def classify(change, ci_lo, ci_hi, comparable=True, n_samples=None):
    """change is the fractional change in *goodness*: positive means head is better.

    When the two sides did not run on the same JMH and JVM, no verdict is issued. The
    difference in measurement apparatus is a systematic bias on every score, not noise, so
    it does not shrink with more rounds and the effect threshold means nothing.

    Too few samples likewise yields no verdict -- see MIN_SAMPLES.
    """
    if not comparable:
        return "unknown", ""
    if n_samples is not None and min(n_samples) < MIN_SAMPLES:
        return "insufficient", ":grey_question: too few samples"
    ci_excludes_unity = not (ci_lo <= 1.0 <= ci_hi)
    if not ci_excludes_unity or abs(change) < MIN_EFFECT:
        return "same", ""
    if change < 0:
        return "regression", ":warning: regression"
    return "improvement", ":rocket: faster"


def format_params(params):
    if not params:
        return "-"
    return "<br>".join("`{}={}`".format(k, v) for k, v in sorted(params.items()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, help="directory of JMH JSON files")
    parser.add_argument("--jmh-args", default="", help="global JMH args used, for the report header")
    parser.add_argument("--rounds", default="?", help="number of alternating rounds")
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--head-sha", default="")
    args = parser.parse_args()

    base, base_toolchain = load_results(args.results, "base")
    head, head_toolchain = load_results(args.results, "head")
    if not base or not head:
        raise SystemExit("No results found under {}".format(args.results))

    comparable = base_toolchain == head_toolchain
    rng = random.Random(SEED)
    rows = []
    for key in sorted(set(base) & set(head)):
        b, h = base[key], head[key]
        b_med = statistics.median(b["samples"])
        h_med = statistics.median(h["samples"])
        if b_med == 0:
            continue
        ci_lo, ci_hi = bootstrap_ratio_ci(b["samples"], h["samples"], rng)
        ratio = h_med / b_med

        # Convert the raw score ratio into a change in goodness so that throughput and
        # average-time benchmarks can be ranked in the same table.
        if higher_is_better(h["mode"], h["unit"]):
            change = ratio - 1.0
        else:
            change = (1.0 / ratio) - 1.0 if ratio else 0.0

        n_samples = (len(b["samples"]), len(h["samples"]))
        verdict, label = classify(change, ci_lo, ci_hi, comparable, n_samples)
        rows.append(
            {
                "name": key[0],
                "params": h["params"],
                "base": b_med,
                "head": h_med,
                "unit": h["unit"],
                "change": change,
                "ci": (ci_lo, ci_hi),
                "verdict": verdict,
                "label": label,
                "n": n_samples,
            }
        )

    # Worst first, so the interesting rows are at the top of a long table.
    rows.sort(key=lambda r: r["change"])

    regressions = [r for r in rows if r["verdict"] == "regression"]
    improvements = [r for r in rows if r["verdict"] == "improvement"]
    starved = [r for r in rows if r["verdict"] == "insufficient"]

    out = sys.stdout
    out.write("<!-- druid-benchmark-report -->\n")
    out.write("## JMH query benchmarks\n\n")
    if not comparable:
        out.write(
            "**No verdicts issued: base and head did not run on the same toolchain.** "
            "(base: {}; head: {}). JMH's blackhole implementation and the JIT both changed, "
            "which shifts every score by a systematic amount that more rounds cannot average "
            "away. The measurements are below for reference, but the ratios mix the code "
            "change with the measurement change and should not be read as a regression "
            "signal.\n\n".format(
                "; ".join(sorted("JMH {} / JVM {}".format(j, v) for j, v in base_toolchain)),
                "; ".join(sorted("JMH {} / JVM {}".format(j, v) for j, v in head_toolchain)),
            )
        )
    elif len(starved) == len(rows):
        out.write(
            "**No verdicts issued: every benchmark produced too few samples.** ".format()
        )
    elif regressions:
        out.write(
            "**{} of {} benchmarks look slower.** ".format(len(regressions), len(rows))
        )
    else:
        # Count only what was actually judged, so a partly-starved run does not read as a
        # clean bill of health for benchmarks nobody measured properly.
        judged = len(rows) - len(starved)
        out.write("**No regressions detected** across {} benchmarks. ".format(judged))
    if improvements:
        out.write(
            "{} look{} faster. ".format(
                len(improvements), "s" if len(improvements) == 1 else ""
            )
        )
    out.write(
        "These numbers come from a shared GitHub runner and are advisory only -- "
        "they never fail the build. Only the base-vs-head *ratio* is meaningful; "
        "absolute scores are not comparable across runs.\n\n"
    )

    if starved:
        out.write(
            "> :grey_question: **{} of {} benchmarks produced fewer than {} samples per side** "
            "and were not judged either way. Raise the measurement iterations or the round "
            "count for those entries; a verdict from this little data would be noise dressed "
            "up as a result.\n\n".format(len(starved), len(rows), MIN_SAMPLES)
        )

    only_base = sorted(set(base) - set(head))
    only_head = sorted(set(head) - set(base))
    if only_base or only_head:
        out.write(
            "> {} benchmark(s) ran only on base and {} only on head; "
            "they are omitted from the table.\n\n".format(len(only_base), len(only_head))
        )

    out.write("| Benchmark | Params | Base | Head | Change | 95% CI on ratio | |\n")
    out.write("|---|---|---:|---:|---:|:---:|---|\n")
    for r in rows:
        short_name = r["name"].replace("org.apache.druid.benchmark.", "")
        out.write(
            "| `{}` | {} | {:.4g} | {:.4g} {} | {:+.1%} | {:.3f} - {:.3f} | {} |\n".format(
                short_name,
                format_params(r["params"]),
                r["base"],
                r["head"],
                r["unit"],
                r["change"],
                r["ci"][0],
                r["ci"][1],
                r["label"],
            )
        )

    # Entries can carry their own -i override, so report the range rather than implying
    # every row was measured the same number of times.
    counts = sorted(n for r in rows for n in r["n"])
    if not counts:
        samples = "0"
    elif counts[0] == counts[-1]:
        samples = str(counts[0])
    else:
        samples = "{}-{}".format(counts[0], counts[-1])
    out.write(
        "\n<sub>Change is stated as improvement in speed: positive is faster. "
        "A row is flagged only when it moves more than {:.0%} *and* its bootstrap CI "
        "excludes 1.0. {} alternating base/head rounds, {} pooled samples per variant, "
        "`{}`.".format(MIN_EFFECT, args.rounds, samples, args.jmh_args.strip())
    )
    if args.base_sha and args.head_sha:
        out.write(" Base `{}` vs head `{}`.".format(args.base_sha[:9], args.head_sha[:9]))
    out.write("</sub>\n")


if __name__ == "__main__":
    main()

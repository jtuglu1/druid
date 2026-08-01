<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# Druid benchmarks

JMH micro-benchmarks for Druid. Benchmark sources live under `src/test/java`, and the module packages
itself into an executable uber-jar via `maven-assembly-plugin`.

## Running locally

Build the jar:

```bash
mvn -B install -q -ff -pl benchmarks -am -P skip-tests -P skip-static-checks -Dweb.console.skip=true -T1C
```

`-P skip-tests` sets `skipTests` but not `maven.test.skip`, so the benchmark classes still compile and
`benchmarks/target/benchmarks.jar` is still produced.

Run a benchmark. JMH forks a fresh JVM per trial and does not inherit surefire's `argLine`, so the
strong-encapsulation flags have to be passed explicitly or the DataSketches, Guice and vector-API
benchmarks will fail on startup:

```bash
JVM_ARGS=$(mvn -q -N help:evaluate -Dexpression=jdk.strong.encapsulation.argLine -DforceStdout | tr -s '[:space:]' ' ')
java $JVM_ARGS -jar benchmarks/target/benchmarks.jar 'VarianceBenchmark' -f 1 -wi 3 -i 5
```

The first positional argument is a regex matched against fully-qualified `Class.method` names. Useful
JMH flags: `-l` to list matching benchmarks without running them, `-p name=value` to override a
`@Param`, `-f/-wi/-i/-w/-r` to override `@Fork`/`@Warmup`/`@Measurement` and their durations, and
`-rf json -rff out.json` to write machine-readable results.

### Cache generated segments

Benchmarks that build data through `SegmentGenerator` regenerate their segments on **every fork**
unless you give them somewhere to cache. Set this and repeat runs get dramatically faster:

```bash
export DRUID_BENCHMARK_CACHE_DIR=/path/to/cache   # or -Ddruid.benchmark.cacheDir=...
```

The cache is keyed on the schema and index spec, not on the Druid build, so wipe it if you are
changing segment-writing code.

## Continuous integration

`.github/workflows/benchmarks.yml` runs a curated subset on pull requests, **opt-in only**: apply the
`benchmark` label to a PR. It builds both the PR head and its merge base, runs the suite in
`.github/benchmarks/pr-suite.txt` alternately against each on the same runner, and posts a table of
head-vs-base ratios.

The two builds run as a parallel matrix and hand their jars to the measurement job as artifacts, so
they overlap rather than running back to back. Only the measurement is pinned to a single runner —
that is what makes base and head comparable — while a jar does not care where it was compiled. The
workflow is independent of the other CI workflows, so it starts as soon as the label lands instead of
queueing behind the unit tests.

### Sharding

The suite is split across parallel shards by cost. Each shard runs base and head for its own
entries, which keeps the comparison valid — the A/B pair for any one benchmark still runs back to
back on a single machine, and results from different benchmarks are never compared against each
other, so they have no reason to share a runner. A final job merges every shard's JSON and renders
one report.

Entries carry an optional `# cost=<seconds>` hint that `.github/scripts/shard_suite.py` packs
longest-first into the least-loaded shard. Without it, one shard ends up holding most of the work,
because entry costs span three orders of magnitude. The hints affect only balance, never results,
so a stale hint costs a little wall clock and nothing else.

To run one shard locally:

```bash
SHARD_INDEX=2 SHARD_TOTAL=3 .github/scripts/run_benchmarks.sh
```

**Shards cannot run concurrently on one machine.** JMH takes a global lock at `$TMPDIR/jmh.lock`
and refuses to start a second instance, which is a good thing — concurrent benchmarks on shared
cores would not be measuring anything meaningful. Run them one after another locally; in CI each
shard has its own runner.

### Vectorized and non-vectorized

Every suite entry that exposes a `vectorize` dimension runs **both** settings, so a change that speeds
up one path while regressing the other cannot hide. Today that covers `ExpressionVectorSelectorBenchmark`,
`FilteredAggregatorBenchmark` and `GroupByBenchmark`. When adding an entry, check whether the class has
a `vectorize` (or `useVectorApi`) `@Param` and keep both values rather than pinning one.

Because the two sides are separate builds, a PR that changes `jmh.version` or the JVM makes them
incomparable — JMH's blackhole mode and inlining behaviour depend on both. The report detects this and
says so at the top rather than presenting misleading ratios.

Results are **advisory and never fail the build**. GitHub's shared runners have no CPU pinning and
noisy neighbours, so absolute scores are not comparable between runs. Only the ratio between the two
builds measured back-to-back on the same machine means anything, and even that carries a noise floor
of a few percent — the report flags a benchmark only when it moves more than 10% *and* its bootstrap
confidence interval excludes 1.0.

## Adding a benchmark to the PR suite

Add a line to `.github/benchmarks/pr-suite.txt`:

```
<jmh-regex>   [extra jmh args]
```

Then keep the cost in check. Each entry runs **four times** per CI job (base and head, times two
alternating rounds), and one run costs roughly:

```
2s JVM fork  +  trial setup  +  (warmup iters x warmup time)  +  (measurement iters x measurement time)
```

With the suite defaults (`-f 1 -wi 5 -i 10 -w 1s -r 1s`) that is ~17s plus setup, so about **70
seconds of wall clock per (method x param-combo) across the whole A/B matrix**. The current 16 entries
expand to 37 such combos, or roughly 45 minutes of JMH. The two builds add about 15 minutes to the
critical path rather than 30, because they run in parallel.

To see what an entry actually costs before adding it, list what it selects and count the parameter
combinations:

```bash
java $JVM_ARGS -jar benchmarks/target/benchmarks.jar -l '<your-regex>'
```

Two things make this easy to get wrong:

- **Method count.** A bare class name selects every `@Benchmark` method in it —
  `LikeFilterBenchmark` has 13, `ExpressionSelectorBenchmark` 15. Narrow with a method regex.
- **`@Param` cross-products.** Every dimension multiplies. `SqlBaseBenchmark` is 192 combinations
  before you multiply by the query list. Pin each dimension with `-p`.

### Shrinking a benchmark without losing signal

What determines whether the A/B comparison can resolve a regression is the width of the confidence
interval on the ratio of medians, which depends on ops per iteration and on the number of pooled
iterations — not on total runtime. Time spent on trial setup, redundant `@Param` combinations, or
warmup past steady state buys nothing.

Free:

1. Set `DRUID_BENCHMARK_CACHE_DIR` (the CI script already does, using a separate directory per
   variant so the head build cannot read base-written segments).
2. Collapse `@Param` cross-products with `-p`. This costs coverage, not resolution — the combinations
   that remain are measured exactly as precisely as before.
3. Narrow multi-method classes with a method regex.

Cheap:

4. Trim over-provisioned iteration counts. Several classes are annotated for a human wanting a
   definitive absolute number: `ExpressionAggregationBenchmark` declares `@Warmup(15) @Measurement(30)`,
   45s per method per combination. Because the comparator pools raw iterations across rounds, `-wi 5
   -i 10` still yields 20 measurements per variant.
5. Reduce `rowsPerSegment` on cursor-level benchmarks, typically 1000000 to 100000. At 100k rows the
   scan loop still dominates cursor construction, so row-loop regressions stay visible.

Do not:

6. Drop below `-wi 2` or `-i 5`. Too few pooled samples and the interval widens past the 10% detection
   threshold, so every row reads as inconclusive.
7. Take cursor benchmarks below ~10k rows — setup and teardown become the thing being measured.
8. Shrink a benchmark whose whole point is scale. `DictionaryEncodedStringIndexSupplierBenchmark`
   exists to exercise the large-dictionary path; cutting its row count removes the effect it measures.

### Verify before you commit

Run the pipeline with the same commit on both sides and confirm the benchmark is stable:

```bash
BASE_SHA=$(git rev-parse HEAD) HEAD_SHA=$(git rev-parse HEAD) .github/scripts/run_benchmarks.sh
```

Every ratio should land near 1.0 with an interval spanning it, and nothing should be flagged. If your
new entry's interval is wider than ±10%, it has been shrunk too far — raise its iteration count or
leave it out.

## Benchmarks that will not run out of the box

- `compression/ColumnarLongsSelectRowsFromSegmentBenchmark` and
  `compression/ColumnarLongsEncodeDataFromSegmentBenchmark` need a real Druid segment supplied via the
  `segmentPath` param, read from `tmp/encoding/%s` relative to the working directory.
- `LongCompressionBenchmark` and `FloatCompressionBenchmark` need their data files generated first by
  the `main()` methods in `LongCompressionBenchmarkFileGenerator` / `FloatCompressionBenchmarkFileGenerator`.
- `IndexedTableLoadingBenchmark` requests a 12 GB heap.
- `MSQWindowFunctionsBenchmark` generates 20,000,000 rows.

None of these belong in the PR suite.

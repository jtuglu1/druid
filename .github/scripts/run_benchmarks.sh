#!/bin/bash

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

# Runs the curated JMH suite in .github/benchmarks/pr-suite.txt against two builds of
# Druid -- the PR head and its merge base -- on the same machine, alternating between
# them so that thermal drift and noisy neighbours affect both equally. Only the ratio
# between the two is reported; absolute scores from a shared CI runner are meaningless.
#
# Required environment, unless BASE_JAR and HEAD_JAR are both supplied:
#   BASE_SHA   commit to use as the baseline
#   HEAD_SHA   commit to use as the candidate
#
# Optional environment:
#   BASE_JAR   prebuilt benchmarks.jar for the baseline; skips building it here
#   HEAD_JAR   prebuilt benchmarks.jar for the candidate; skips building it here
#   JMH_ARGS      global JMH args, overriding the @Warmup/@Measurement/@Fork annotations
#   ROUNDS        number of alternating base/head rounds (default 2)
#   SUITE         path to the suite file
#   WORK_DIR      scratch directory for jars, segment caches and JSON results
#   SHARD_TOTAL   number of shards the suite is split across (default 1)
#   SHARD_INDEX   1-based shard this invocation runs (default 1)
#   SKIP_COMPARE  set to "true" to emit JSON only, leaving comparison to a later job

set -e
set -x
# JMH output is piped through tee, so without this a failing java would be masked by tee's
# exit status.
set -o pipefail

BASE_JAR="${BASE_JAR:-}"
HEAD_JAR="${HEAD_JAR:-}"
JMH_ARGS="${JMH_ARGS:--f 1 -wi 5 -i 10 -w 1s -r 1s}"
ROUNDS="${ROUNDS:-2}"
SUITE="${SUITE:-.github/benchmarks/pr-suite.txt}"
WORK_DIR="${WORK_DIR:-/tmp/bench}"
SHARD_TOTAL="${SHARD_TOTAL:-1}"
SHARD_INDEX="${SHARD_INDEX:-1}"
SKIP_COMPARE="${SKIP_COMPARE:-false}"

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

# Two modes:
#   - CI passes BASE_JAR and HEAD_JAR, built by parallel jobs, and nothing is compiled here.
#   - Locally, BASE_SHA/HEAD_SHA are given and both jars are built in sequence.
BUILD_LOCALLY=false
if [ -z "$BASE_JAR" ] || [ -z "$HEAD_JAR" ]; then
  BUILD_LOCALLY=true
  BASE_SHA="${BASE_SHA:?set BASE_SHA/HEAD_SHA, or supply prebuilt BASE_JAR/HEAD_JAR}"
  HEAD_SHA="${HEAD_SHA:?set BASE_SHA/HEAD_SHA, or supply prebuilt BASE_JAR/HEAD_JAR}"

  # Building both variants moves HEAD around, so refuse to run on a dirty tree and put the
  # checkout back where it started on the way out.
  if [ -n "$(git status --porcelain)" ]; then
    echo "Working tree is dirty; commit or stash before running benchmarks." >&2
    exit 1
  fi
  ORIGINAL_REF="$(git symbolic-ref --quiet --short HEAD || git rev-parse HEAD)"
  trap 'git checkout --quiet "$ORIGINAL_REF"' EXIT
fi

OUT_DIR="$WORK_DIR/out"
rm -rf "$WORK_DIR"
mkdir -p "$OUT_DIR" "$WORK_DIR/cache/base" "$WORK_DIR/cache/head"

# The strong-encapsulation flags live in exactly one place (pom.xml,
# jdk.strong.encapsulation.argLine). JMH forks a fresh JVM per trial and does not inherit
# surefire's argLine, so they have to be passed explicitly or the DataSketches, Guice and
# vector-API benchmarks fail on startup. Read them from the pom rather than copying them.
JVM_ARGS="$(mvn -q -N help:evaluate -Dexpression=jdk.strong.encapsulation.argLine -DforceStdout \
  | grep -v '^\[' | tr -s '[:space:]' ' ')"

if [ "$BUILD_LOCALLY" = true ]; then
  # build_benchmark_jar.sh cleans before building, which matters here because both variants
  # share one working tree: stale classes or a stale JMH-generated META-INF/BenchmarkList
  # from the previous variant would silently corrupt the comparison.
  git checkout --quiet --detach "$HEAD_SHA"
  .github/scripts/build_benchmark_jar.sh "$WORK_DIR/head.jar"
  git checkout --quiet --detach "$BASE_SHA"
  .github/scripts/build_benchmark_jar.sh "$WORK_DIR/base.jar"
  git checkout --quiet --detach "$HEAD_SHA"
else
  cp "$BASE_JAR" "$WORK_DIR/base.jar"
  cp "$HEAD_JAR" "$WORK_DIR/head.jar"
fi

# Resolve this shard's entries. Every shard computes the same cost-balanced split from the
# same file, so no coordination between runners is needed. A single shard is the whole suite.
# Written to a temp file rather than a bash array to stay compatible with bash 3.2, which is
# what macOS ships and what contributors run this on locally.
ENTRIES_FILE="$WORK_DIR/entries.txt"
python3 .github/scripts/shard_suite.py \
  --suite "$SUITE" --shards "$SHARD_TOTAL" --index "$SHARD_INDEX" --summary \
  > "$ENTRIES_FILE"
if [ ! -s "$ENTRIES_FILE" ]; then
  echo "Shard $SHARD_INDEX/$SHARD_TOTAL of $SUITE is empty" >&2
  exit 1
fi

run_entry() {
  # $1: variant, $2: round, $3: index, $4...: jmh regex and per-entry args
  local variant="$1" round="$2" idx="$3"
  shift 3
  # The shard number is part of the filename so that results merged from every shard into a
  # single directory cannot collide -- entry indexes restart at 1 within each shard.
  local result="$OUT_DIR/${variant}-s${SHARD_INDEX}-r${round}-e${idx}.json"

  # Segment caches are per-variant on purpose. SegmentGenerator keys its cache on the
  # schema/spec inputs only, not on the Druid build, so a shared cache would let the head
  # build read base-written segments and hide segment-format changes.
  local log="$OUT_DIR/${variant}-s${SHARD_INDEX}-r${round}-e${idx}.log"

  DRUID_BENCHMARK_CACHE_DIR="$WORK_DIR/cache/$variant" \
    java $JVM_ARGS -jar "$WORK_DIR/$variant.jar" "$@" $JMH_ARGS -rf json -rff "$result" \
    2>&1 | tee "$log"

  # A regex that matches nothing makes JMH exit 0 having measured nothing, which would
  # silently produce an empty report. Treat it as a failure.
  if [ ! -s "$result" ] || ! grep -q '"benchmark"' "$result"; then
    echo "Benchmark selector '$1' matched no benchmarks in the $variant jar" >&2
    exit 1
  fi

  # JMH also exits 0 when an individual benchmark throws -- it just drops that method from
  # the results. That would silently remove a row from the report rather than flagging it,
  # so treat a thrown benchmark as a failure. Known-broken benchmarks are kept out of the
  # suite file instead.
  if grep -q '<failure>' "$log"; then
    echo "A benchmark threw while running '$1' on $variant; see $log" >&2
    exit 1
  fi
}

for round in $(seq 1 "$ROUNDS"); do
  idx=0
  while IFS= read -r entry; do
    idx=$((idx + 1))
    # Honour shell quoting in the suite file so params containing spaces survive, but
    # keep pathname expansion off: benchmark selectors are regexes like "matchHalf.*"
    # and must not be rewritten into filenames that happen to match.
    set -f
    eval "entry_args=($entry)"
    set +f
    run_entry base "$round" "$idx" "${entry_args[@]}"
    run_entry head "$round" "$idx" "${entry_args[@]}"
  done < "$ENTRIES_FILE"
done

# When sharded, each shard emits only its slice of the JSON and a later job merges every
# shard's results before comparing -- a single shard has no business rendering a report that
# claims to cover the whole suite.
if [ "$SKIP_COMPARE" = true ]; then
  set +x
  echo "Shard $SHARD_INDEX/$SHARD_TOTAL complete; results in $OUT_DIR"
  exit 0
fi

# Record the PR number for the companion workflow that posts the comment; the benchmark
# job itself has a read-only token on fork PRs and cannot comment.
if [ -n "${PR_NUMBER:-}" ]; then
  echo "$PR_NUMBER" > "$WORK_DIR/pr-number.txt"
fi

set +x
python3 .github/scripts/compare_benchmarks.py \
  --results "$OUT_DIR" \
  --jmh-args "$JMH_ARGS" \
  --rounds "$ROUNDS" \
  --base-sha "${BASE_SHA:-}" \
  --head-sha "${HEAD_SHA:-}" \
  > "$WORK_DIR/comment.md"

cat "$WORK_DIR/comment.md"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  cat "$WORK_DIR/comment.md" >> "$GITHUB_STEP_SUMMARY"
fi

# Advisory only: a detected regression never fails the job. Build failures, JMH crashes
# and empty selectors above still do.
exit 0

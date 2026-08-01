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

# Builds benchmarks.jar from whatever is currently checked out and copies it to $1.
#
# The benchmark workflow runs this once per variant in parallel jobs, so the two builds
# overlap instead of running back to back. The measurement itself still happens on a single
# runner -- only the builds are distributed, since a jar does not care where it was compiled.

set -e
set -x

OUT="${1:?usage: build_benchmark_jar.sh <output-jar-path>}"

cd "$(git rev-parse --show-toplevel)"

# "clean" guards against a dirty target/ from a previous build in the same workspace; the
# JMH annotation processor writes META-INF/BenchmarkList, and a stale copy silently
# misrepresents which benchmarks exist.
mvn -B clean install -q -ff -pl benchmarks -am \
  -P skip-tests -P skip-static-checks \
  -Dweb.console.skip=true -Dmaven.javadoc.skip=true -Dcyclonedx.skip=true \
  -T1C

# -P skip-tests sets skipTests but not maven.test.skip, so test-classes still compile and
# maven-assembly-plugin still produces the uber-jar.
mkdir -p "$(dirname "$OUT")"
cp benchmarks/target/benchmarks.jar "$OUT"

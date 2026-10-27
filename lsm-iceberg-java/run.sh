#!/usr/bin/env bash
# Usage: ./run.sh demo | inspect [table] [warehouse] [n] | bench [--flag value ...] | test
set -euo pipefail
cd "$(dirname "$0")"
if [[ "${1:-}" == "test" ]]; then exec mvn -q test; fi
if [[ ! -f target/classpath.txt || pom.xml -nt target/classpath.txt ]]; then
  mvn -q dependency:build-classpath -Dmdep.outputFile=target/classpath.txt >/dev/null
fi
mvn -q compile
CP="target/classes:$(cat target/classpath.txt)"
cmd="${1:-demo}"; shift || true
case "$cmd" in
  demo)    exec java -cp "$CP" cs631.lsm.tools.Demo "$@" ;;
  inspect) exec java -cp "$CP" cs631.lsm.tools.InspectMetadata "$@" ;;
  bench)   exec java -Xmx2g -cp "$CP" cs631.lsm.tools.Benchmark "$@" ;;
  *) echo "usage: ./run.sh demo | inspect [table] [warehouse] [n] | bench [--keys N ...] | test"; exit 1 ;;
esac

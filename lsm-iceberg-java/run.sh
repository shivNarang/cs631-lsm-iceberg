#!/usr/bin/env bash
# Usage: ./run.sh demo | inspect [table] [warehouse] [n] | bench [--flag value ...] | eval | usecase | spark | test
set -euo pipefail
cd "$(dirname "$0")"
if [[ "${1:-}" == "test" ]]; then
  mkdir -p target; status=0; mvn -q test > target/test-output.txt 2>&1 || status=$?
  grep -v "^WARNING" target/test-output.txt || true
  grep -h "Tests run" target/surefire-reports/*.txt | sed "s/ -- in cs631.lsm./  /"
  exit $status
fi
if [[ ! -f target/classpath.txt || pom.xml -nt target/classpath.txt ]]; then
  mvn -q dependency:build-classpath -Dmdep.outputFile=target/classpath.txt >/dev/null
fi
mvn -q compile
CP="target/classes:$(cat target/classpath.txt)"
cmd="${1:-demo}"; shift || true
case "$cmd" in
  demo)    exec java -cp "$CP" cs631.lsm.tools.Demo "$@" ;;
  inspect) exec java -cp "$CP" cs631.lsm.tools.InspectMetadata "$@" ;;
  bench)   exec java -Xmx3g -cp "$CP" cs631.lsm.tools.Benchmark "$@" ;;
  eval)    exec java -Xmx3g -cp "$CP" cs631.lsm.tools.Benchmark --keys 100000 --updates 400000 --batch 25000 \
             --lookups 500 --scans 50 --ycsb-ops 20000 --time-limit 20 --out evaluation \
             --stores lsm,lsm-parquet,lsm-fence,lsm-nocompact,append,mor,cow "$@" ;;
  usecase) exec java -cp "$CP" cs631.lsm.tools.UseCase "$@" ;;
  spark)   cd spark
           if [[ ! -f target/classpath.txt || pom.xml -nt target/classpath.txt ]]; then
             mvn -q dependency:build-classpath -Dmdep.outputFile=target/classpath.txt >/dev/null
           fi
           mvn -q compile
           exec java -Xmx2g --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
             --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED \
             --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
             --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
             --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
             --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED \
             --add-opens=java.base/sun.util.calendar=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false \
             -cp "target/classes:$(cat target/classpath.txt)" cs631.spark.SparkUseCase "$@" ;;
  *) echo "usage: ./run.sh demo | inspect [table] [warehouse] [n] | bench [--keys N ...] | eval | usecase | spark | test"; exit 1 ;;
esac

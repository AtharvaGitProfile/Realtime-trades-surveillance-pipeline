#!/usr/bin/env bash
# setup.sh
# Downloads required Flink connector JARs into /opt/flink/lib/ before
# the JobManager or TaskManager process starts.

set -euo pipefail

FLINK_LIB=/opt/flink/lib
MAVEN_CENTRAL="https://repo1.maven.org/maven2"

declare -A JARS=(
    ["flink-sql-connector-kafka-3.1.0-1.18.jar"]="org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"
)

echo "=== Flink connector setup ==="

for jar in "${!JARS[@]}"; do
    dest="$FLINK_LIB/$jar"
    if [ -f "$dest" ]; then
        echo "  [skip] $jar already present"
    else
        url="$MAVEN_CENTRAL/${JARS[$jar]}"
        echo "  [download] $jar"
        curl -fSL --retry 3 --retry-delay 2 -o "$dest" "$url"
        echo "  [ok] $jar"
    fi
done

echo "=== Setup complete. Starting Flink: $* ==="
exec /docker-entrypoint.sh "$@"

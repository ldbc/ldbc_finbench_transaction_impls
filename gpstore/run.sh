#!/usr/bin/env bash
# Usage: ./run.sh [config-name]
#   config-name: properties file name (without .properties) under src/main/resources/
#                default: example_benchmark
#   e.g. ./run.sh example_benchmark
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

CONFIG="${1:-example_benchmark}"
GPSTORE_JAR="target/gpstore-0.2.0-alpha.jar"
GPSTORE_SDK_JAR="lib/gpstore-api-sdk-0.0.1-alpha.jar"

for required_jar in "${GPSTORE_JAR}" "${GPSTORE_SDK_JAR}"; do
  if [ ! -f "${required_jar}" ]; then
    echo "Missing required JAR: ${SCRIPT_DIR}/${required_jar}" >&2
    exit 1
  fi
done

java -cp "${GPSTORE_JAR}:${GPSTORE_SDK_JAR}" \
  org.ldbcouncil.finbench.driver.driver.Driver \
  -P "src/main/resources/${CONFIG}.properties"

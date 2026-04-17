#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SYNTHEA_DIR="${PROJECT_ROOT}/synthea"
SYNTHEA_JAR="${SYNTHEA_DIR}/synthea-with-dependencies.jar"
SYNTHEA_VERSION="${SYNTHEA_VERSION:-v4.0.0}"
PATIENT_COUNT="${1:-1000}"
BREW_JAVA_BIN="/opt/homebrew/opt/openjdk@17/bin/java"

if [[ -n "${JAVA_BIN:-}" && -x "${JAVA_BIN}" ]]; then
  JAVA_CMD="${JAVA_BIN}"
elif [[ -x "${BREW_JAVA_BIN}" ]]; then
  JAVA_CMD="${BREW_JAVA_BIN}"
elif command -v java >/dev/null 2>&1; then
  JAVA_CMD="$(command -v java)"
else
  echo "Java 17+ is required but no java executable was found." >&2
  exit 1
fi

JAVA_MAJOR="$("${JAVA_CMD}" -version 2>&1 | sed -n '1s/.*version \"\([0-9][0-9]*\).*/\1/p')"
if [[ -z "${JAVA_MAJOR}" || "${JAVA_MAJOR}" -lt 17 ]]; then
  echo "Java 17+ is required for Synthea ${SYNTHEA_VERSION}. Found: ${JAVA_MAJOR:-unknown}" >&2
  exit 1
fi

mkdir -p "${SYNTHEA_DIR}"

if [[ ! -f "${SYNTHEA_JAR}" ]]; then
  curl -fL \
    "https://github.com/synthetichealth/synthea/releases/download/${SYNTHEA_VERSION}/synthea-with-dependencies.jar" \
    -o "${SYNTHEA_JAR}"
fi

rm -rf "${SYNTHEA_DIR}/output"

(
  cd "${SYNTHEA_DIR}"
  "${JAVA_CMD}" -jar "${SYNTHEA_JAR}" \
    -p "${PATIENT_COUNT}" \
    --exporter.fhir.export=true \
    --exporter.csv.export=false
)

PATIENT_FILES="$(find "${SYNTHEA_DIR}/output/fhir" -maxdepth 1 -type f -name '*.json' ! -name '*Information*.json' | wc -l | tr -d ' ')"
echo "FHIR output is available in ${SYNTHEA_DIR}/output/fhir"
echo "Generated ${PATIENT_FILES} patient bundle files."
echo "Synthea targets ${PATIENT_COUNT} living patients and may add deceased records for realism."

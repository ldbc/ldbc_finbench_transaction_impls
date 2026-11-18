#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${1:-${SCRIPT_DIR}/../../../ldbc-finbench-sf30-latest}"
DATA_DIR="${2:-${SCRIPT_DIR}/../data}"
TARGET_DIR="${DATA_DIR}/sf30"

for required_file in \
  MD5SUMS.txt \
  sf30.tar.gz.md5 \
  sf30.tar.gz.part00 \
  sf30.tar.gz.part01; do
  if [ ! -f "${SOURCE_DIR}/${required_file}" ]; then
    echo "Missing required file: ${SOURCE_DIR}/${required_file}" >&2
    exit 1
  fi
done

if [ -e "${TARGET_DIR}" ]; then
  echo "Refusing to overwrite existing directory: ${TARGET_DIR}" >&2
  exit 1
fi

(
  cd "${SOURCE_DIR}"
  md5sum -c MD5SUMS.txt
)

combined_md5="$(
  cat "${SOURCE_DIR}/sf30.tar.gz.part00" \
      "${SOURCE_DIR}/sf30.tar.gz.part01" |
  md5sum |
  awk '{print $1}'
)"
expected_md5="$(awk '{print $1}' "${SOURCE_DIR}/sf30.tar.gz.md5")"
if [ "${combined_md5}" != "${expected_md5}" ]; then
  echo "Combined archive checksum mismatch" >&2
  exit 1
fi

mkdir -p "${DATA_DIR}"
STAGING_DIR="$(mktemp -d "${DATA_DIR}/.sf30-staging.XXXXXX")"
cleanup() {
  rm -rf "${STAGING_DIR}"
}
trap cleanup EXIT

if command -v unpigz >/dev/null 2>&1; then
  cat "${SOURCE_DIR}/sf30.tar.gz.part00" \
      "${SOURCE_DIR}/sf30.tar.gz.part01" |
    unpigz -dc |
    tar -xf - -C "${STAGING_DIR}"
else
  cat "${SOURCE_DIR}/sf30.tar.gz.part00" \
      "${SOURCE_DIR}/sf30.tar.gz.part01" |
    gzip -dc |
    tar -xf - -C "${STAGING_DIR}"
fi

STAGED_SF30_DIR="${STAGING_DIR}/sf30"
for required_dir in factor_table incremental read_params snapshot; do
  if [ ! -d "${STAGED_SF30_DIR}/${required_dir}" ]; then
    echo "Missing extracted directory: ${STAGED_SF30_DIR}/${required_dir}" >&2
    exit 1
  fi
done

shopt -s nullglob
raw_csv_files=("${STAGED_SF30_DIR}/snapshot"/*.csv)
if [ "${#raw_csv_files[@]}" -ne 18 ]; then
  echo "Expected 18 raw snapshot CSV files, found ${#raw_csv_files[@]}" >&2
  exit 1
fi

mv "${STAGED_SF30_DIR}" "${TARGET_DIR}"
trap - EXIT
rm -rf "${STAGING_DIR}"

echo "Extracted complete sf30 dataset to ${TARGET_DIR}"

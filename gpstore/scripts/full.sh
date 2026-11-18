#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTERMEDIATE_DIR="${SCRIPT_DIR}/../data/intermediate"

# The conversion script applies the UTC+8 offset explicitly.
export TZ=Asia/Shanghai

# pwd = snapshot/
mkdir -p content_datetime
shopt -s nullglob
csv_files=(./*.csv)
if [ "${#csv_files[@]}" -ne 18 ]; then
  echo "Expected 18 top-level snapshot CSV files, found ${#csv_files[@]} in $(pwd)." >&2
  exit 1
fi
mv "${csv_files[@]}" content_datetime
python3 "${SCRIPT_DIR}/convert_headers.py"
sed -i '1d' content_datetime/*.csv
rm -rf intermediate
cp -r "${INTERMEDIATE_DIR}" intermediate
python3 "${SCRIPT_DIR}/write_col_type.py"
python3 "${SCRIPT_DIR}/convert_time_to_long.py"
rm -rf intermediate

for output_dir in content_datetime content_long headers orig_headers; do
  count="$(find "${output_dir}" -maxdepth 1 -type f -name '*.csv' | wc -l)"
  if [ "${count}" -ne 18 ]; then
    echo "Expected 18 CSV files in ${output_dir}, found ${count}" >&2
    exit 1
  fi
done

echo "Converted snapshot data in $(pwd)"

#!/usr/bin/env bash
set -euo pipefail

# pwd = snapshot/
mkdir -p content_datetime
shopt -s nullglob
csv_files=(./*.csv)
if [ ${#csv_files[@]} -eq 0 ]; then
  echo "No top-level CSV files found in $(pwd). Put the raw snapshot CSV files here before running full.sh." >&2
  exit 1
fi
mv "${csv_files[@]}" content_datetime
python3 scripts/convert_headers.py
sed -i 1d content_datetime/*.csv
rm -rf intermediate
cp -r ../../intermediate .
python3 scripts/write_col_type.py
python3 scripts/validate_generated_headers.py
python3 scripts/convert_time_to_long.py

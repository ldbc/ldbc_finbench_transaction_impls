# pwd = snapshot/
mkdir content_datetime
mv *.csv content_datetime
python3 scripts/convert_headers.py
sed -i 1d content_datetime/*.csv
cp -r ../../intermediate .
python3 scripts/write_col_type.py
python3 scripts/convert_time_to_long.py
#!/bin/bash

cwd=$(cd $(dirname $0) && pwd)
queries_path=${cwd}/../queries

for file in "${queries_path}"/*.gsql; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        gsql ${queries_path}/${filename}
    fi
done
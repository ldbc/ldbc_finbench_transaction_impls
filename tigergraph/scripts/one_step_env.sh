#!/bin/bash

DATA_SIZE=${1:-1}
SINGLE_FLAG=${1:-false}

cwd=$(cd $(dirname $0) && pwd)
# 1.define schema and loading job
bash ${cwd}/setup_schema.sh

# 2.load data into TigerGraph
bash ${cwd}/load_data.sh ${DATA_SIZE}

# 3.create queries
bash ${cwd}/create_queries.sh

# 4.install queries (default is single_GPR mode)
bash ${cwd}/install_queries.sh ${SINGLE_FLAG}
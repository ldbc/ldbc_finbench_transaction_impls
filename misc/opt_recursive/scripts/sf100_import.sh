SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Import SF100
cd /root/datasets/sf100/snapshot
lgraph_import -c ${SCRIPT_DIR}/import.conf --overwrite 1 \
    --dir /root/lgraph_db_sf100 --delimiter "|" --v3 0

cd $SCRIPT_DIR

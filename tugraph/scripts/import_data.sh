SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR/../data/${1}/snapshot
lgraph_import -c ../../import.conf --overwrite 1 --dir /data/lgraph_db --delimiter "|" --v3 0

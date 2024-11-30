ENDPOINT="tugraph:7070"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROCEDURES_DIR=/root/ldbc/ldbc_finbench_transaction_impls/tugraph/procedures/cpp

cd $PROCEDURES_DIR

for i in trw1 trw2 trw3; do
    python3 install.py $ENDPOINT $i RW
done
for i in tcr8; do
    python3 install.py $ENDPOINT $i RO
done

cd $SCRIPT_DIR

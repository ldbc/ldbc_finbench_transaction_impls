ENDPOINT="127.0.0.1:7071"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR/../procedures/cpp
for i in trw1 trw2 trw3; do
    python3 install.py $ENDPOINT $i RW
done
for i in tcr1 tcr8; do
    python3 install.py $ENDPOINT $i RO
done
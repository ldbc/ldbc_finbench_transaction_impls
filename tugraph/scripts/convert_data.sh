SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR
cp -r $SCRIPT_DIR/../data/${1}/snapshot $SCRIPT_DIR/../data/${1}/snapshot.bak
python3 convert_data.py $SCRIPT_DIR/../data/${1}/snapshot.bak $SCRIPT_DIR/../data/${1}/snapshot

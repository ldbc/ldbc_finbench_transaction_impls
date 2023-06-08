INCLUDE_DIR="/usr/local/include"
LIBLGRAPH="/usr/local/lib64/liblgraph.so"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR/../procedures/cpp
for i in trw1 trw2 trw3; do
    g++ -fno-gnu-unique -fPIC -g --std=c++17 -I$INCLUDE_DIR -rdynamic -O3 -fopenmp -o $i.so $i.cpp $LIBLGRAPH -shared
done
for i in tcr8; do
    g++ -fno-gnu-unique -fPIC -g --std=c++17 -I$INCLUDE_DIR -rdynamic -O3 -fopenmp -o $i.so $i.cpp $LIBLGRAPH -shared
done

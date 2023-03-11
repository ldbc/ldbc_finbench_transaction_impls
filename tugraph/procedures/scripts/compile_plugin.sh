INCLUDE_DIR=/usr/local/include
LIBLGRAPH=/usr/local/lib64/liblgraph.so
g++ -fno-gnu-unique -fPIC -g --std=c++14 -I../deps/hopscotch-map/include -I../deps/date/include -I$INCLUDE_DIR -rdynamic -O3 -fopenmp -o $1.so $1.cpp $LIBLGRAPH -shared

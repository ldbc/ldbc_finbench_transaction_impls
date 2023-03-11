export endpoint="127.0.0.1:7071"
for i in `seq 1 14`; do ./compile_plugin.sh transaction_complex_read_$i; python install.py $endpoint transaction_complex_read_$i RO; done
for i in `seq 1 7`; do ./compile_plugin.sh transaction_simple_read_$i; python install.py $endpoint transaction_simple_read_$i RO; done
for i in `seq 1 8`; do ./compile_plugin.sh transaction_update_$i; python install.py $endpoint transaction_update_$i RW; done

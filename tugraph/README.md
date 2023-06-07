# 1. Preparation

## 1.1 Set environment variables

```Bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
export LD_LIBRARY_PATH=/usr/local/lib64:${JAVA_HOME}/jre/lib/amd64/server
export PATH=$PATH:/data/apache-maven-3.9.2/bin
export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8
```

## 1.2 Download & install dependencies

```bash
# install driver
cd /data
git clone https://github.com/ldbc/ldbc_finbench_driver.git
cd /data/ldbc_finbench_driver
mvn install

# install impl
cd /data
git clone https://github.com/ldbc/ldbc_finbench_transaction_impls.git
cd /data/ldbc_finbench_transaction_impls && git checkout tugraph-cypher
mvn clean package

# install tugraph
mkdir /data/ldbc_finbench_transaction_impls/tugraph/deps
cd /data/ldbc_finbench_transaction_impls/tugraph/deps
sudo dpkg -i tugraph-3.4.0-1.x86_64.deb

# install g++
cd /data/ldbc_finbench_transaction_impls/tugraph/deps
wget http://ftp.gnu.org/gnu/gcc/gcc-8.4.0/gcc-8.4.0.tar.gz
tar zxf gcc-8.4.0.tar.gz && cd gcc-8.4.0
./contrib/download_prerequisites && mkdir build && cd build
../configure CFLAGS=-fPIC CXXFLAGS=-fPIC -enable-checking=release -enable-languages=c,c++ -disable-multilib
make -j4 && sudo make install

# install procedure
bash /data/ldbc_finbench_transaction_impls/tugraph/scripts/build_procedure.sh

# install acid
cd /data/ldbc_finbench_transaction_impls/tugraph/deps
git clone https://github.com/ldbc/ldbc_finbench_acid.git
```

# 2. Loading and preprocessing

## 2.1 Data

```bash
/data/ldbc_finbench_transaction_impls/tugraph/data
├── sf1
│   ├── incremental
│   ├── read_params
│   └── snapshot
└── sf10
    ├── incremental
    ├── read_params
    └── snapshot
```

## 2.2 Load data

```bash
# stop server
cd /data && lgraph_server --directory /data/lgraph_db --log_dir /data/lgraph_log -d stop
# load data
bash /data/ldbc_finbench_transaction_impls/tugraph/scripts/import_data.sh sf1
```

## 2.3 Install stored procedure

```Bash
# start server
cd /data && lgraph_server --directory /data/lgraph_db --log_dir /data/lgraph_log -d start
# install procedure
time bash /data/ldbc_finbench_transaction_impls/tugraph/scripts/load_procedure.sh
```

# 3. Benchmark

## 3.1 Validate

### 3.1.1 Create validation

```shell
# create validation
cd /data/ldbc_finbench_transaction_impls/tugraph/deps/ldbc_finbench_transaction_impls/tugraph && sync
bash run.sh create_validation.properties
# stop server
cd /data && lgraph_server --directory /data/lgraph_db --log_dir /data/lgraph_log -d stop
```

### 3.1.2 Validate

```shell
# copy (neo4j) validation_params.csv
cd /data/tugraph_ldbc_snb/deps/ldbc_finbench_transaction_impls/tugraph && sync
cp ${VALIDATION_PARAMS_PATH}/validation_params.csv ./
# validate
bash run.sh validate_database.properties
# stop server
cd /data && lgraph_server --directory /data/lgraph_db --log_dir /data/lgraph_log -d stop
```

## 3.2 Run benchmark

```shell
# warmup db
lgraph_warmup -d ${DB_ROOT_DIR}/lgraph_db -g default
# run benchmark
cd /data/tugraph_ldbc_snb/deps/ldbc_finbench_transaction_impls/tugraph && sync
bash run.sh benchmark.properties
# stop server
cd /data && lgraph_server --directory /data/lgraph_db --log_dir /data/lgraph_log -d stop
```

# 4. ACID Tests

## 4.1 Compile & run ACID tests

ACID test implementations are reviewed for compliance with the ACID test specification and implement all specified test cases. Additionally, tests execute successfully without atomicity and isolation test failures, supporting serializable isolation level transaction settings.

```shell
cd /data/ldbc_finbench_transaction_impls/tugraph/deps/ldbc_finbench_acid/tugraph
bash compile_embedded.sh acid
./acid
```
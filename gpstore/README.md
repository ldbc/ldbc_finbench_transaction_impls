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
cd /data/ldbc_finbench_transaction_impls
mvn clean package

# install acid
cd /data
git clone https://github.com/ldbc/ldbc_finbench_acid.git
cd /data/ldbc_finbench_acid
mvn clean package
```


# 2. Loading and preprocessing

## 2.1 Data

download Finbench data, list data directories

```bash
$ tree -d /data/ldbc_finbench_transaction_impls/gpstore/data
/data/ldbc_finbench_transaction_impls/gpstore/data
├── sf1
│   ├── incremental
│   ├── intermedidate
│   ├── read_params
│   └── snapshot
└── sf10
    ├── incremental
    ├── intermediate
    ├── read_params
    └── snapshot
```

## 2.2 Convert data

Use sf10 as an example. (Same below without further specification)

```bash
# pwd ldbc_finbench_transaction_impls/gpstore
cp -r scripts data/sf10/snapshot
mv data/sf10/intermediate data/sf10/snapshot
cd data/sf10/snapshot
sh scripts/full.sh
```

## 2.3 Load data

```bash
# pwd ldbc_finbench_transaction_impls/gpstore
cd gpstore_isolate_test
rm -rf finbench-sf10
sh finbench10_splitPredicate.sh
```

## 2.4 Build plugins

```bash
# pwd ldbc_finbench_transaction_impls/gpstore
cd gpstore_isolate_test/plugins
# Please remember to modify `path_pref` in build.py to be the absolute path of ldbc_finbench_transaction_impls/gpstore/gpstore_isolate_test on your machine before the next step
python3 build.py
```

## 2.5 Start GPStoreServer

```bash
# pwd ldbc_finbench_transaction_impls/gpstore
cd gpstore_isolate_test
bin/grpc -db finbench-sf10 -p <port_no.>
```

Please remember to modify the port number in the respective `.properties` file before executing the next step, whether it be validation or benchmarking.

# 3. Benchmark

## 3.1 Validate

### 3.1.1 Create validation

```shell
# create validation
cd /data/ldbc_finbench_transaction_impls/gpstore && sync
bash sf1_finbench_create_validation.sh
```

### 3.1.2 Validate

```shell
cd /data/ldbc_finbench_transaction_impls/gpstore && sync
bash sf1_finbench_validation_database.sh
```

## 3.2 Run benchmark

```shell
# run benchmark
cd /data/ldbc_finbench_transaction_impls/gpstore && sync
bash sf10_finbench_benchmark.sh
```

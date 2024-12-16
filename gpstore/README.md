# 1. Preparation

## 1.1 Set environment variables

The following absolute paths are for reference only; please adjust them according to the installation paths on your machine.

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
```

Please download our compressed docker image `gpstore-finbench-docker.tar.gz` via [this link](https://disk.pku.edu.cn/link/AA387A00AC18BB464FA58F26F3C37E5CE0).

# 2. Loading and preprocessing

## 2.1 Data

Download FinBench data, decompress it and integrate it into the existing `data/` directory so that the directory hierarchy looks like the following:

```bash
$ tree -d /data/ldbc_finbench_transaction_impls/gpstore/data
/data/ldbc_finbench_transaction_impls/gpstore/data
├── finbench-sf1
│   ├── incremental
│   ├── intermedidate
│   ├── read_params
│   └── snapshot
└── finbench-sf10
    ├── incremental
    ├── intermediate
    ├── read_params
    └── snapshot
```

## 2.2 Convert data

Use sf10 as an example. (Same below without further specification)

```bash
# pwd ldbc_finbench_transaction_impls/gpstore
cp -r scripts data/finbench-sf10/snapshot
mv data/finbench-sf10/intermediate data/sf10/snapshot
cd data/finbench-sf10/snapshot
sh scripts/full.sh
```

## 2.3 Docker image installation & startup

```bash
# Load the image
tar -xzvf gpstore-finbench-docker.tar.gz
docker load -i gpstore-finbench-docker.tar
docker image list	# Check that you see REPOSITORY gpstore & TAG finbench

# Startup the container
# Please feel free to replace 9009 by any other available port on your machine; change driver's configuration accordingly (We assume the port is 9090 in the following instructions)
# path/to/datasets: absolute path to ldbc_finbench_transaction_impls/gpstore/data/
# path/to/logs: any path that you have read & write access to
docker run -itd -p 9009:9000 --privileged --cap-add=SYS_PTRACE \
--security-opt seccomp=unconfined --restart always \
-e TZ=Asia/Shanghai \
-v path/to/datasets:/gpstore/datasets \
-v path/to/logs:/gpstore/logs \
--name gpstore-server gpstore:finbench
docker ps -a	# Check the STATUS of gpstore-server; it should be UP
```

Please remember to modify the port number in the respective `.properties` file before executing the validation or benchmarking step.

## 2.4 Build plugins [OPTIONAL]

The plugins have been built and deployed inside the docker container; the code is the same as those under the `ldbc_finbench_transaction_impls/gpstore/plugins` path.

Therefore, the following steps are not mandatory. Please only execute them when you have modified one of the plugins and wish to check its effect.

```bash
# Copy the modified plugin into the container
docker cp new_plugin_file_path gpstore-server:/gpstore/plugins
# Build the plugin inside the container
docker exec -it gpstore-server /bin/bash
cd plugins
python3 build.py
# Restart the container to let the modified plugin take effect
exit
docker restart gpstore-server
```

## 2.3 Load data

```bash
curl -X POST -H 'Content-Type: application/json' -d '{"operation":"build","username":"root","password":"123456","db_name":"finbench-sf10"}' http://127.0.0.1:9009/grpc/api
curl -X POST -H 'Content-Type: application/json' -d '{"operation":"load","username":"root","password":"123456","db_name":"finbench-sf10"}' http://127.0.0.1:9009/grpc/api
```

# 3. Benchmark

## 3.1 Validate

### 3.1.1 Create validation

```shell
# create validation
cd /data/ldbc_finbench_transaction_impls/gpstore && sync
bash sf10_finbench_create_validation.sh
```

### 3.1.2 Validate

```shell
cd /data/ldbc_finbench_transaction_impls/gpstore && sync
bash sf10_finbench_validation_database.sh
```

## 3.2 Run benchmark

```shell
# run benchmark
cd /data/ldbc_finbench_transaction_impls/gpstore && sync
bash sf10_finbench_benchmark.sh
```

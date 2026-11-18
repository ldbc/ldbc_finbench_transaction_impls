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

In the following instructions, `${FINBENCH_HOME}` denotes the FinBench installation root.


## 1.2 Download & install dependencies

```bash
# install driver
cd ${FINBENCH_HOME}
git clone https://github.com/ldbc/ldbc_finbench_driver.git
cd ${FINBENCH_HOME}/ldbc_finbench_driver
mvn install

# install impl
cd ${FINBENCH_HOME}
git clone https://github.com/ldbc/ldbc_finbench_transaction_impls.git
cd ${FINBENCH_HOME}/ldbc_finbench_transaction_impls

git checkout feature/optimization-recommendation        # remove this line after merging feature/optimization-recommendation into main

mvn clean package -DskipTests

# compile gpstore only
# mvn -pl gpstore -am clean package -DskipTests
```

Please download our compressed docker image `gpstore-finbench-261113.tar.gz` via [this link](https://disk.pku.edu.cn/link/AA60B89B60ED6B444B8A348F3CC9B238FD).

# 2. Loading and preprocessing

## 2.1 Download, Verify, and Extract Data

The following instructions use sf30 as an example unless stated otherwise.

Download the sf30 dataset from [this link](https://www.modelscope.cn/datasets/GDC/ldbc-finbench-sf30-latest). The downloaded directory has the following structure:

```text
ldbc-finbench-sf30-latest
├── MD5SUMS.txt
├── README.md
├── sf30.tar.gz.md5
├── sf30.tar.gz.part00
└── sf30.tar.gz.part01
```

The following command verifies the two archive parts and the combined archive, concatenates them in order, and extracts the complete dataset to `ldbc_finbench_transaction_impls/gpstore/data/sf30`:

```bash
cd ${FINBENCH_HOME}
bash ldbc_finbench_transaction_impls/gpstore/scripts/extract_sf30.sh \
  "${FINBENCH_HOME}/ldbc-finbench-sf30-latest"
```

At this point, `ldbc_finbench_transaction_impls/gpstore/data/sf30` contains:

```text
data/sf30
├── factor_table/
├── incremental/
├── read_params/
└── snapshot/            # 18 raw snapshot CSV files
```

The `read_params`, `incremental`, and `factor_table` directories are already complete and should not be modified in the next step.

## 2.2 Convert Snapshot Data to GPStore Format

```bash
cd ${FINBENCH_HOME}/ldbc_finbench_transaction_impls/gpstore/data/sf30/snapshot
bash ../../../scripts/full.sh
```

The resulting data directory must have the following layout:

```text
data/sf30
├── factor_table/
├── incremental/
├── read_params/
└── snapshot/
    ├── content_datetime/    # data rows without headers
    ├── content_long/        # data rows with millisecond timestamps
    ├── headers/             # GPStore node/relationship headers
    └── orig_headers/        # original column names
```

## 2.3 Docker image installation & startup

```bash
# Load the image
tar -xzvf gpstore-finbench-261113.tar.gz
docker load -i gpstore-finbench-261113.tar
docker image list	# Check that you see REPOSITORY gpstore & TAG finbench

# Startup the container
# Please feel free to replace 9022 by any other available port on your machine; change driver's configuration accordingly (We assume the port is 9022 in the following instructions)
# path/to/datasets: absolute path to ldbc_finbench_transaction_impls/gpstore/data/
# path/to/logs: any path that you have read & write access to
docker run -itd -p 9022:9000 --privileged --cap-add=SYS_PTRACE \
--security-opt seccomp=unconfined --restart always \
-e TZ=Asia/Shanghai \
-v path/to/datasets:/gpstore/datasets \
-v path/to/logs:/gpstore/logs \
--name gpstore-server gpstore:finbench-261113
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

## 2.5 Build & load database

```bash
# Build database
curl -X POST -H 'Content-Type: application/json' -d '{"operation":"build","username":"root","password":"123456","db_name":"sf30"}' http://127.0.0.1:9022/grpc/api
# Load database
curl -X POST -H 'Content-Type: application/json' -d '{"operation":"load","username":"root","password":"123456","db_name":"sf30"}' http://127.0.0.1:9022/grpc/api
```

Building the database can take a long time. We recommend running the build command with `nohup` and sending it to the background with `&`.


### 2.5.1 Back up database [OPTIONAL] [RECOMMENDED]

The following steps are optional but recommended. They allow you to avoid rebuilding the database when creating a new container.

Copy the built database from the Docker container as a backup.

```bash
# After the database has been built
# curl -X POST -H 'Content-Type: application/json' -d '{"operation":"build","username":"root","password":"123456","db_name":"sf30"}' http://127.0.0.1:9022/grpc/api

cd ${FINBENCH_HOME}/ldbc_finbench_transaction_impls/gpstore
mkdir -p backup
docker cp gpstore-server:/gpstore/sf30.db ./backup/sf30.db
```

Restore the backup into a new or existing container.

The database directory and its metadata are stored separately:

- `/gpstore/sf30.db` contains the database data.
- `/gpstore/system.db` records that `sf30` has been built.

Stop the server before removing or copying the database. Removing a loaded database can invalidate pointers held by running query threads and terminate the server.

```bash
docker stop gpstore-server

# Remove any old or partial database directory.
docker run --rm --volumes-from gpstore-server \
  --entrypoint /bin/bash gpstore:finbench-261113 -lc \
  'rm -rf /gpstore/sf30.db'

# Copy the backup into the stopped server container.
docker cp ./backup/sf30.db gpstore-server:/gpstore/sf30.db

# Register sf30 in system.db. backup.json is missing from some packaged images and must exist before ginit calls add_backuplog().
docker run --rm --volumes-from gpstore-server \
  --entrypoint /bin/bash gpstore:finbench-261113 -lc \
  'cd /gpstore
   if [ ! -s backup.json ]; then
     printf "{\"db_name\":\"system\",\"backup_timer\":\"600\"}\n" > backup.json
   fi
   bin/ginit -db sf30'

# Start the server and load the registered backup.
docker start gpstore-server
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"operation":"load","username":"root","password":"123456","db_name":"sf30"}' \
  http://127.0.0.1:9022/grpc/api

# Check that sf30 is registered and loaded.
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"operation":"show","username":"root","password":"123456"}' \
  http://127.0.0.1:9022/grpc/api
```

# 3. Benchmark

Running the driver usually takes a long time. We recommend redirecting its output with `>>` and running it in the background with `&`.

## 3.1 Validate

Please note that, typically, *either 3.1.1 or 3.1.2 will be executed across a run*, since they serve different purposes:

- "Create validation" is for generating a `validation_params.csv` file from gpstore's outputs, so as this CSV file can be used as the ground truth to validate other systems.
- "Validate" is for validating gpstore against an existing, correct `validation_params.csv` file, such as that provided under this directory.

### 3.1.1 Create validation

```shell
cd ${FINBENCH_HOME}/ldbc_finbench_transaction_impls/gpstore && sync
bash run.sh example_create_validation
```

### 3.1.2 Validate

```shell
cd ${FINBENCH_HOME}/ldbc_finbench_transaction_impls/gpstore && sync
bash run.sh example_validate
```

## 3.2 Run Benchmark

```shell
cd ${FINBENCH_HOME}/ldbc_finbench_transaction_impls/gpstore && sync
bash run.sh example_benchmark
```

## 3.3 Run Optimization Recommendation

```shell
cd ${FINBENCH_HOME}/ldbc_finbench_transaction_impls/gpstore && sync
bash run.sh example_opt
```

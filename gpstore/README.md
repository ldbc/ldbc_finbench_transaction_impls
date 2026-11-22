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

mvn clean package -DskipTests

# compile gpstore only
# mvn -pl gpstore -am clean package -DskipTests
```

Please download our compressed docker image `gpstore-finbench-261120.tar.gz` via [this link](https://disk.pku.edu.cn/link/AA4BED26770E41446BA6C02863D8FDF4D0).

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
cd /data
bash ldbc_finbench_transaction_impls/gpstore/scripts/extract_sf30.sh \
  /data/ldbc-finbench-sf30-latest
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
cd /data/ldbc_finbench_transaction_impls/gpstore/data/sf30/snapshot
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
tar -xzvf gpstore-finbench-261120.tar.gz
docker load -i gpstore-finbench-261120.tar
docker image list	# Check that gpstore:finbench-261120 is present

# Startup the container
# Replace 9022 with another available host port if necessary and update the
# driver configuration accordingly.
# path/to/datasets: absolute path to ldbc_finbench_transaction_impls/gpstore/data/
# path/to/logs: any path that you have read & write access to
docker run -itd -p 9022:9000 --privileged --cap-add=SYS_PTRACE \
  --security-opt seccomp=unconfined --restart always \
  -e TZ=Asia/Shanghai \
  -v path/to/datasets:/gpstore/datasets \
  -v path/to/logs:/gpstore/logs \
  --name gpstore-server gpstore:finbench-261120
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
curl -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "operation": "build",
    "username": "root",
    "password": "123456",
    "db_name": "sf30"
  }' \
  http://127.0.0.1:9022/grpc/api

# Load database
curl -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "operation": "load",
    "username": "root",
    "password": "123456",
    "db_name": "sf30"
  }' \
  http://127.0.0.1:9022/grpc/api
```

Building the database can take a long time. We recommend running the build command with `nohup` and sending it to the background with `&`.

### 2.5.1 Back up and restore within the same container

Use `createResetSnapshot` and `restoreResetSnapshot` when the container will not be replaced and the database must be restored to a known state between benchmark runs:

```bash
# Before the first benchmark run, capture the current clean database.
curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "operation": "createResetSnapshot",
    "username": "root",
    "password": "123456",
    "db_name": "sf30"
  }' \
  http://127.0.0.1:9022/grpc/api

# Restore the snapshot after a workload modifies the database.
curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "operation": "restoreResetSnapshot",
    "username": "root",
    "password": "123456",
    "db_name": "sf30"
  }' \
  http://127.0.0.1:9022/grpc/api
```

`createResetSnapshot` returns after the container-local snapshot `backups/<db_name>.db.reset` has been created. It captures the current database state, so create it only when the database is clean. `restoreResetSnapshot` copies that snapshot, unloads the current database, installs the copy, and reloads the database before returning; the snapshot itself is never modified. No manual `unload` or `load` request is required.

If needed, call `createResetSnapshot` again to replace the snapshot with a new state.

### 2.5.2 Export a backup and restore it into a new container

The reset snapshot is fixed to the current container. Before deleting or replacing the container, create a timestamped portable backup:

```bash
# The database must be loaded when backup is called.
curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "operation": "backup",
    "username": "root",
    "password": "123456",
    "db_name": "sf30",
    "backup_path": "/gpstore/backups"
  }' \
  http://127.0.0.1:9022/grpc/api
```

The response contains a `backupfilepath`, for example:

```json
{
  "StatusCode": 0,
  "StatusMsg": "Database backup successfully.",
  "backupfilepath": "/gpstore/backups/sf30.db_YYMMDDHHMMSS"
}
```

Make sure that no workload is running, then export both the timestamped backup and `/gpstore/system.db` before removing the old container:

```bash
cd /data/ldbc_finbench_transaction_impls/gpstore
mkdir -p backup

docker cp \
  gpstore-server:/gpstore/backups/sf30.db_YYMMDDHHMMSS \
  ./backup/sf30.db_YYMMDDHHMMSS

docker cp \
  gpstore-server:/gpstore/system.db \
  ./backup/system.db

# you can stop & delete the container now
docker stop gpstore-server
docker rm gpstore-server
```

The system database stores the registration and permissions of the built database. The database data and `system.db` must be restored together.

Create, but do not start, a new container:

```bash
docker create -p 9022:9000 --privileged --cap-add=SYS_PTRACE \
  --security-opt seccomp=unconfined --restart always \
  -e TZ=Asia/Shanghai \
  -v path/to/datasets:/gpstore/datasets \
  -v path/to/logs:/gpstore/logs \
  --name gpstore-server gpstore:finbench-261120
```

Import the database and its registration before the first server start:

```bash
docker cp ./backup/system.db/. gpstore-server:/gpstore/system.db

docker cp ./backup/sf30.db_YYMMDDHHMMSS gpstore-server:/gpstore/sf30.db
```

Start the server, load the restored database, and verify its status:

```bash
docker start gpstore-server

curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "operation": "load",
    "username": "root",
    "password": "123456",
    "db_name": "sf30"
  }' \
  http://127.0.0.1:9022/grpc/api

curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "operation": "show",
    "username": "root",
    "password": "123456"
  }' \
  http://127.0.0.1:9022/grpc/api
```

Do not copy only `/gpstore/<db_name>.db` into a new container and call `load`. The data files then exist, but the fresh container's system database does not know that the database was built:

```json
{"StatusCode":1004,"StatusMsg":"The database [sf30] not built yet."}
```

Importing the exported `system.db` together with the backup avoids this error. After loading the restored database, create a new reset snapshot if repeated same-container resets are needed:

```bash
curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "operation": "createResetSnapshot",
    "username": "root",
    "password": "123456",
    "db_name": "sf30"
  }' \
  http://127.0.0.1:9022/grpc/api
```

# 3. Benchmark

Running the driver usually takes a long time. We recommend running with `nohup`, redirecting its output with `>>` and running it in the background with `&`.

## 3.1 Validate

Please note that, typically, *either 3.1.1 or 3.1.2 will be executed across a run*, since they serve different purposes:

- "Create validation" is for generating a `validation_params.csv` file from gpstore's outputs, so as this CSV file can be used as the ground truth to validate other systems.
- "Validate" is for validating gpstore against an existing, correct `validation_params.csv` file, such as that provided under this directory.

### 3.1.1 Create validation

```shell
cd /data/ldbc_finbench_transaction_impls/gpstore && sync
bash run.sh example_create_validation
```

### 3.1.2 Validate

```shell
cd /data/ldbc_finbench_transaction_impls/gpstore && sync
bash run.sh example_validate
```

## 3.2 Run Benchmark

```shell
cd /data/ldbc_finbench_transaction_impls/gpstore && sync
bash run.sh example_benchmark
```

## 3.3 Run Optimization Recommendation

```shell
cd /data/ldbc_finbench_transaction_impls/gpstore && sync
bash run.sh example_opt
```

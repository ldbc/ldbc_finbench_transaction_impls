# README

This document elaborates how to run the experiments for choke-points analysis.

## Environment and Datasets

These two experiments are conducted in docker.
- Docker image: `tugraph/tugraph-compile-centos8:1.3.1`
- Hardware: Alibaba Cloud `ecs.r6.8xlarge`, 32xIntel Xeon Platinum 8269CY vCPUs, 256GiB RAM

FinBench Datasets:
- [SF 0.1](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf0.1/sf0.1.tar.xz) with [md5 checksum](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf0.1/sf0.1.tar.xz.md5sum)
- [SF 1](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf1/sf1.tar.xz) with [md5 checksum](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf1/sf1.tar.xz.md5sum)
- [SF 3](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf3/sf3.tar.xz) with [md5 checksum](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf3/sf3.tar.xz.md5sum)
- [SF 10](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf10/sf10.tar.xz) with [md5 checksum](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf10/sf10.tar.xz.md5sum)
- [SF 30](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf30/sf30.tar.xz) with [md5 checksum](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf30/sf30.tar.xz.md5sum)
- [SF 100](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf100/sf100.tar.xz) with [md5 checksum](oss://tugraph-web/tugraph/datasets/finbench/v0.2.0/sf100/sf100.tar.xz.md5sum)

## Experiment 1: Temporal Locality in Storage

The temporal locality experiment is executed on TuGraph-DB with FinBench SF100 dataset. The following steps show how to reproduce the temporal locality experiment. Execute following steps under current doc's directory `ldbc_finbench_transaction_impls/misc`.

### 1. Download Experiment Resources

Download temporal locality experiment package into current directory and download the FinBench sf100 dataset into experiment package directory.

```shell
$ wget https://tugraph-web.oss-cn-beijing.aliyuncs.com/tugraph/datasets/finbench/v0.2.0/experiments/temporal_locality.zip
$ unzip temporal_locality.zip
$ cd ./temporal_locality
$ wget https://tugraph-web.oss-cn-beijing.aliyuncs.com/tugraph/datasets/finbench/v0.2.0/sf100/sf100.tar.xz
$ tar -xf ./sf100.tar.xz
$ cd ..
```

### 2. Update Source Code in Query Implementation

The experiment uses procedure to execute `ComplexRead1` instead of Cypher, which requires a modification in the `ComplexRead1Handler` in `ldbc_finbench_transaction_impls/tugraph/src/main/java/org/ldbcouncil/finbench/impls/tugraph/TuGraphTransactionDb.java:100` with the following code.

```java
public static class ComplexRead1Handler implements OperationHandler<ComplexRead1, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead1 cr1, TuGraphDbConnectionState dbConnectionState,
                ResultReporter resultReporter) throws DbException {
            try {
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                String cypher = "CALL plugin.cpp.tcr1({ id:%d, startTime: %d, endTime: %d, truncationLimit: %d});";
                long startTime = cr1.getStartTime().getTime();
                long endTime = cr1.getEndTime().getTime();
                int truncationLimit = cr1.getTruncationLimit();
                cypher = String.format(
                        cypher,
                        cr1.getId(), startTime, endTime, truncationLimit);
                String graph = "default";
                String res = client.callCypher(cypher, graph, 0);
                ArrayList<ComplexRead1Result> results = new ArrayList<>();
                JSONArray array = JSONObject.parseArray(res);
                for (int i = 0; i < array.size() - 1; i++) {
                    JSONObject ob = array.getJSONObject(i);
                    ComplexRead1Result result = new ComplexRead1Result(
                            ob.getLongValue("otherId"),
                            ob.getIntValue("accountDistance"),
                            ob.getLongValue("mediumId"),
                            ob.getString("mediumType"));
                    results.add(result);
                }
                resultReporter.report(results.size(), results, cr1);
                dbConnectionState.pushClient(client);

                JSONObject ob = array.getJSONObject(array.size() - 1);
                System.out.printf("iterationTime: %f, executionTime: %f%n", ob.getDoubleValue("iterationTime"), ob.getDoubleValue("executionTime"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
```

### 3. Compile Benchmark Implementation

Compile the implementation.

```shell
$ cd ..
$ mvn clean package
$ cd ./misc
```

### 4. Install TuGraph-DB in Docker Container

To run temporal locality experiment in docker `tugraph/tugraph-compile-centos8:1.3.1`, install `tugraph-4.0.0-1.el8.x86_64.rpm` first.

```shell
$ docker pull tugraph/tugraph-compile-centos8:1.3.1
$ docker run -it -v ./temporal_locality:/root/temporal_locality -p 7071:7071 -p 9091:9091 --name=finbench_temporal_loaclity tugraph/tugraph-compile-centos8:1.3.1 /bin/bash
$ cd /root/temporal_locality/
$ rpm -ivh tugraph-4.0.0-1.el8.x86_64.rpm --nodeps
```

Use the following command to check if it is successfully installed, whose output is like this. 

```shell
$ lgraph_server --version
TuGraph v4.0.0, compiled from "finbench" branch, commit "335335f" (web commit "8253763").
  CPP compiler version: "GNU" "8.4.0".
  Python version : "3.6.9".
```

### 5. Preprocess SF100 Dataset

Before importing SF100 dataset，we need to preprocess the snapshot of SF100 dataset in terms of the data format. This can be done outside of docker container.

```shell
$ cd ./temporal_locality/sf100
$ mv ./snapshot ./snapshot.bak
$ mkdir ./snapshot
$ cd ..
$ python convert_data.py ./sf100/snapshot.bak ./sf100/snapshot
$ cd ..
```


### 6. Run Baseline Version

Import SF100 dataset with temporal sorting feature disabled. Then start TuGraph-DB server.

```shell
$ docker exec -it finbench_temporal_loaclity /bin/bash
$ cd /root/temporal_locality/sf100/snapshot
$ lgraph_import -c /root/temporal_locality/baseline/import.conf --overwrite 1 --dir /root/lgraph_db_sf100 --delimiter "|" --v3 0
$ cd /root/temporal_locality/
$ lgraph_server -c /root/temporal_locality/lgraph_sf100.json -d start
$ exit # exit from the container
```

Load `ComplexRead1` procedure (baseline version) for baseline experiment. Then link sf100 dataset to `./data` directory, and run benchmark

```shell
$ cp ./temporal_locality/load_procedure_temporal.sh ../tugraph/scripts
$ chmod u+x ../tugraph/scripts/load_procedure_temporal.sh
$ cp ./temporal_locality/baseline/tcr1.cpp ../tugraph/procedures/cpp
$ ../tugraph/scripts/load_procedure_temporal.sh
$ cd ../tugraph/
$ cd ./data
$ ln -s ../../misc/temporal_locality/sf100 ./sf100
$ cd ..
$ bash run.sh ../misc/temporal_locality/benchmark.properties > temporal_locality.log
$ cd ../misc
```

The execution and iteration time in `ComplexRead1` is recorded in `temporal_locality.log`.
Collect and analyze the elapsed time with `./temporal_locality/log_data_collector.py`

```shell
$ cd ./temporal_locality
$ python log_data_collector.py ../../tugraph/temporal_locality.log
$ cd ..
```

Stop TuGraph-DB server.

```shell
$ docker exec -it finbench_temporal_loaclity /bin/bash
$ cd /root/temporal_locality/
$ lgraph_server -d stop
$ exit
```

### 7. Run Optimized Version

Import SF100 dataset with temporal sorting feature enabled. Then start TuGraph-DB server.

```shell
$ docker exec -it finbench_temporal_loaclity /bin/bash
$ cd /root/temporal_locality/sf100/snapshot
$ lgraph_import -c /root/temporal_locality/optimized/import.conf --overwrite 1 --dir /root/lgraph_db_sf100 --delimiter "|" --v3 0
$ cd /root/temporal_locality/
$ lgraph_server -c /root/temporal_locality/lgraph_sf100.json -d start
$ exit # exit from the container
```

Load `ComplexRead1` procedure (optimized version) for optimized experiment. Then link sf100 dataset to `./data` directory, then run temporal locality benchmark.

```shell
$ cp ./temporal_locality/load_procedure_temporal.sh ../tugraph/scripts
$ chmod u+x ../tugraph/scripts/load_procedure_temporal.sh
$ cp ./temporal_locality/optimized/tcr1.cpp ../tugraph/procedures/cpp
$ ../tugraph/scripts/load_procedure_temporal.sh
$ cd ../tugraph/
$ cd ./data
$ ln -s ../../misc/temporal_locality/sf100 ./sf100
$ cd ..
$ bash run.sh ../misc/temporal_locality/benchmark.properties > temporal_locality.log
$ cd ../misc
```

The execution and iteration time in `ComplexRead1` is recorded in `temporal_locality.log`.
Collect and analyze the information with `./temporal_locality/log_data_collector.py`

```shell
$ cd ./temporal_locality
$ python log_data_collector.py ../../tugraph/temporal_locality.log
$ cd ..
```

Stop TuGraph-DB server.

```shell
$ docker exec -it finbench_temporal_loaclity /bin/bash
$ cd /root/temporal_locality/
$ lgraph_server -d stop
$ exit
```

## Experiment 2: Recursive Path Filtering

### 1. Preparation

Download SF100 dataset and parameter. Move them to the dataset directory, e.g., the directory `/data/dataset`.

Modify docker-compose.yml to attach local file to volume `/root/datasets/` in container, for example, 
```
volumes: 
    - /data/dataset:/root/datasets/
    - /data/scripts:/root/scripts/
```

Start docker containers with `docker-compose up -d`. After that, two containers start.
- [directory name]_tugraph_1
- [directory name]_finbench_1


### 2. Benchmark Baseline

- Attach to the `xxx_tugraph_1` container. Then install the package with `rpm -i base.rpm`.
- In the container `xxx_tugraph_1`, Enter the `/root/scripts` directory. Import data with `bash sf100_import.sh` and start the server with `bash sf100_start.sh`
- Switch to another container `xxx_finbench_1`. Enter the `/root/scripts` directory. Install C++ plugins with `bash load_procedure.sh`
- In the container `xxx_finbench_1`, copy the file `benchmark_sf100.properties` in scripts to the directory `/root/ldbc/ldbc_finbench_transaction_impls/tugraph/benchmark.properties`.
- In the container `xxx_finbench_1`,  enter the `/root/scripts` directory and run the benchmark with `bash sf100_benchmark.sh`.

### 3. Benchmark Optimization

- Attach to the `xxx_tugraph_1` container. Uninstall the baseline package with `rpm -e tugraph-4.0.0-1.x86_64` and then install the package with `rpm -i opt.rpm`.
- In the container `xxx_tugraph_1`, enter the `/root/scripts` directory. Delete the old data directory with `rm -r /root/lgraph_db_sf100` and `rm -r /root/lgraph_log_sf100`.
- Import data with `bash sf100_import.sh` and start the server with `bash sf100_start.sh`
- Switch to another container `xxx_finbench_1`. Enter the `/root/scripts` directory. Install C++ plugins with `bash load_procedure.sh`
- In the container `xxx_finbench_1`, copy the file `benchmark_sf100.properties` in scripts to the directory
  `/root/ldbc/ldbc_finbench_transaction_impls/tugraph/benchmark.properties`.
- Comment the line in the file `sf100_benchmark.properties`.
```
# cd data && ln -s /root/datasets/sf100 ./sf100 && cd ..
```
- In the container `xxx_finbench_1`, enter the `/root/scripts` directory and run the benchmark with `bash sf100_benchmark.sh`.

## Experiment 3: End to End Evaluation

- [Galaxybase Official Website](https://createlink.com)

### 1. Resources

- Download the [`galaxybase.zip`] file from [here](https://drive.google.com/file/d/1euXCtu-oEzeh6M3Z4mP6LxofbhaYHHxs/view?usp=sharing), unzip it, and you will find `galaxybase.tar.gz` and the JSON files.
- Prepare the data files for both `sf10` and `sf100` datasets.

### 2. Installation & Data Loading

- **Package Extraction**

Extract the Galaxybase package:

```bash
tar -zxf galaxybase.tar.gz
```

- **Environment Setup and Image Installation**

Install the Galaxybase environment and necessary Docker images:

```bash
./galaxybase-*/bin/galaxybase-deploy install docker
./galaxybase-*/bin/galaxybase-deploy image install
```

- **Service Container Deployment**

Deploy the `graph` service containers. 

```shell
./galaxybase-*/bin/galaxybase-deploy build graph --home home
```

- **Validation & Start-Up**

To validate the service, retrieve the verification code using the following command (replace `CONTAINER_ID` with the actual container ID):

```
docker exec -i CONTAINER_ID gtools graph auth-check
```

Then, input the authorization code:

```
docker exec -i CONTAINER_ID gtools graph auth --code `AUTH_CODE`
```

*Note: You can acquire the authorization code by contacting the support team at support@createlink.com.*

- **Data Transfer**

Move the benchmark data to the `home/graph/data` directory. Example for the `sf10` dataset:

```shell
mv sf10/snapshot home/graph/data/sf10
```

- **Data Loading**

Load the `sf10` dataset into Galaxybase using the provided schema and mapping files:

```shell
./galaxybase-*/bin/galaxybase-load -s json/schema_sf10.json -m json/mapping_sf10.json -g sf10
```

### 3. Benchmark Execution

- **Compilation**

First, clone the repository and compile the benchmark implementation. 

```shell
git clone https://github.com/ldbc/ldbc_finbench_transaction_impls 
mv sf10_finbench_benchmark.* ldbc_finbench_transaction_impls/
mv sf100_finbench_benchmark.* ldbc_finbench_transaction_impls/
cd ldbc_finbench_transaction_impls
mvn install -DskipTests
cd galaxybase-cypher
```

- **Benchmark Run**

Execute the benchmark for both `sf10` and `sf100` datasets. 

```shell
# Run benchmark for sf10
nohup sh sf10_finbench_benchmark.sh > console.log &

# Run benchmark for sf100
nohup sh sf100_finbench_benchmark.sh > console.log &
```

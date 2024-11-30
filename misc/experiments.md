# README

This README elaborates how to reproduce the experiments for choke-points analysis.

Note: These two experiments are conducted in docker.

Docker image: tugraph/tugraph-compile-centos8:1.3.1

Hardware: Alibaba Cloud ecs.r6.8xlarge 32xIntel Xeon Platinum 8269CY vCPUs, 256GiB RAM 

# Temporal Locality in Storage

The temporal loacality experiment is executed on TuGraph-DB with FinBench SF100 dataset.

## Resources

- Package: https://tugraph-web.oss-cn-beijing.aliyuncs.com/tugraph/datasets/finbench/v0.2.0/experiments/temporal_locality.zip
    
- Datasets: https://tugraph-web.oss-cn-beijing.aliyuncs.com/tugraph/datasets/finbench/v0.2.0/sf100/sf100.tar.xz

## Experiment Steps

Execute following steps under current doc's directory, `ldbc_finbench_transaction_impls/misc`.

### Download Experiment Resources

#### Download Experiment Package
Download temporal locality experiment package into current directory.

```shell
$ wget https://tugraph-web.oss-cn-beijing.aliyuncs.com/tugraph/datasets/finbench/v0.2.0/experiments/temporal_locality.zip
$ unzip temporal_locality.zip
```

#### Download FinBench SF100 Dataset
Download the FinBench sf100 dataset into experiment package directory

```shell
$ cd ./temporal_locality
$ wget https://tugraph-web.oss-cn-beijing.aliyuncs.com/tugraph/datasets/finbench/v0.2.0/sf100/sf100.tar.xz
$ tar -xf ./sf100.tar.xz
$ cd ..
```

### Run Experiment

The following steps show how to reproduce the temporal locality experiment.

#### Update Source Code and Compile Benchmark

The temporal locality experiment uses procedure instead of cypher to execute ComplexRead1, this requires a modification in the source code of tugraph's implementation. Please update the ComplexRead1Handler in `ldbc_finbench_transaction_impls/tugraph/src/main/java/org/ldbcouncil/finbench/impls/tugraph/TuGraphTransactionDb.java`(start from line: 100) with the following code.

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

Compile the implementation.

```shell
$ cd ..
$ mvn clean package
$ cd ./misc
```

#### Start Docker Container

Docker image `tugraph/tugraph-compile-centos8:1.3.1` is required to run tugraph-db for temporal locality experiment.

```shell
$ docker pull tugraph/tugraph-compile-centos8:1.3.1
$ docker run -it -v ./temporal_locality:/root/temporal_locality -p 7071:7071 -p 9091:9091 --name=finbench_temporal_loaclity tugraph/tugraph-compile-centos8:1.3.1 /bin/bash
$ exit
```

#### Install TuGraph-DB

Install tugraph-db with `tugraph-4.0.0-1.el8.x86_64.rpm` inside docker container.

```shell
$ docker exec -it finbench_temporal_loaclity /bin/bash
$ cd /root/temporal_locality/
$ rpm -ivh tugraph-4.0.0-1.el8.x86_64.rpm --nodeps
```

use the following command to test if tugraph-db is successfully compile and installed.
```shell
$ lgraph_server --version
```

the output should be like this.

```shell
TuGraph v4.0.0, compiled from "finbench" branch, commit "335335f" (web commit "8253763").
  CPP compiler version: "GNU" "8.4.0".
  Python version : "3.6.9".
```

if installed successfully, exit current container

```shell
$ exit
```

#### Import FinBench SF100 Data

To import sf100 dataset into tugraph-db，the snapshot of sf100 dataset must be converted into compatible data format. This can be done outside of docker container.

```shell
$ cd ./temporal_locality/sf100
$ mv ./snapshot ./snapshot.bak
$ mkdir ./snapshot
$ cd ..
$ python convert_data.py ./sf100/snapshot.bak ./sf100/snapshot
$ cd ..
```

##### Baseline Mode Data Import
Import the snapshot of sf100 dataset into tugraph-db(baseline).

```shell
$ docker exec -it finbench_temporal_loaclity /bin/bash
$ cd /root/temporal_locality/sf100/snapshot
$ lgraph_import -c /root/temporal_locality/baseline/import.conf --overwrite 1 --dir /root/lgraph_db_sf100 --delimiter "|" --v3 0
$ exit
```

##### Optimized Mode Data Import
Import the snapshot of sf100 dataset into tugraph-db(optimized).

```shell
$ docker exec -it finbench_temporal_loaclity /bin/bash
$ cd /root/temporal_locality/sf100/snapshot
$ lgraph_import -c /root/temporal_locality/optimized/import.conf --overwrite 1 --dir /root/lgraph_db_sf100 --delimiter "|" --v3 0
$ exit
```

#### Start TuGraph-DB

Start tugraph-db server.

```shell
$ docker exec -it finbench_temporal_loaclity /bin/bash
$ cd /root/temporal_locality/
$ lgraph_server -c /root/temporal_locality/lgraph_sf100.json -d start
$ exit
```

#### Load Procedure

Load ComplexRead1's procedure for temporal locality experiment. First copy `load_procedure_temporal.sh` to `ldbc_finbench_transaction_impls/tugraph/scripts`

```shell
$ cp ./temporal_locality/load_procedure_temporal.sh ../tugraph/scripts
$ chmod u+x ../tugraph/scripts/load_procedure_temporal.sh
```

##### Load Baseline Procedure

```shell
$ cp ./temporal_locality/baseline/tcr1.cpp ../tugraph/procedures/cpp
$ ../tugraph/scripts/load_procedure_temporal.sh
```

##### Load Optimized Procedure

```shell
$ cp ./temporal_locality/optimized/tcr1.cpp ../tugraph/procedures/cpp
$ ../tugraph/scripts/load_procedure_temporal.sh
```

#### Run Benchmark

Go to `ldbc_finbench_transaction_impls/tugraph`, link sf100 dataset to `./data` directory, then run temporal locality benchmark

```shell
$ cd ../tugraph/
$ cd ./data
$ ln -s ../../misc/temporal_locality/sf100 ./sf100
$ cd ..
$ bash run.sh ../misc/temporal_locality/benchmark.properties > temporal_locality.log
$ cd ../misc
```

#### Collect Log Data

The execution time and iteration time in ComplexRead1 is recorded in `temporal_locality.log`, collect and analyze the information with `./temporal_locality/log_data_collector.py`

```shell
$ cd ./temporal_locality
$ python log_data_collector.py ../../tugraph/temporal_locality.log
$ cd ..
```

#### Stop TuGraph-DB

Stop tugraph-db server.

```shell
$ docker exec -it finbench_temporal_loaclity /bin/bash
$ cd /root/temporal_locality/
$ lgraph_server -d stop
$ exit
```

# Recursive Path Filtering

## Resources

- Pull Request:
- Package:
    - Baseline: 
    - Optimized:
- Datasets:
    - SF100：https://tugraph-web.oss-cn-beijing.aliyuncs.com/tugraph/datasets/finbench/v0.2.0/sf100/sf100.tar.xz
    - Parameter of SF100：https://tugraph-web.oss-cn-beijing.aliyuncs.com/tugraph/datasets/finbench/v0.2.0/sf100/substitute_parameters.tar.gz.

## Experiment Steps

### Move to directory
Download dataset and parameter and move them to the dataset directory, for example, the directory '/data/dataset'.

Modify docker-compose.yml to attach local file to volume '/root/datasets/' in container, for example, 
```
volumes: 
    - /data/dataset:/root/datasets/
    - ./scripts:/root/scripts/
```

Start docker containers.
```
docker-compose up -d
```

After that, will see two containers.
- [directory name]_tugraph_1
- [directory name]_finbench_1

Benchmark is based on a large-scale SF100 dataset, and the following scripts are required.

### Install package
First, attach to the 'finbench-easyrun_tugraph_1' container.

Then install the package.
```
rpm -i base.rpm
```

### Import data
Enter the '/root/scripts' directory.
```
bash sf100_import.sh
```

### Start tugraph server
Also in the same container 'finbench-easyrun_tugraph_1' and same directory'/root/scripts', start the server.
```
bash sf100_start.sh
```

### C++ Plugin
Switch to another container 'finbench'.

Enter the '/root/scripts' directory.
```
bash load_procedure.sh
```
To install C++ plugins.

### Benchmark Baseline
Before benchmark, in the container 'finbench', copy the file 'benchmark_sf100.properties' in scripts to the directory '/root/ldbc/ldbc_finbench_transaction_impls/tugraph/benchmark.properties'.

Also in the 'finbench' container and enter the '/root/scripts' directory.
```
bash sf100_benchmark.sh
```

### Benchmark Opt
Attach to container 'tugraph'.

Uninstall the baseline package.
```
rpm -e tugraph-4.0.0-1.x86_64
```
Install the opt version.
```
rpm -i opt.rpm
```

Switch to '/root/scripts'.

Delete the old data directory.

```
rm -r /root/lgraph_db_sf100
rm -r /root/lgraph_log_sf100

bash sf100_import.sh
bash sf100_start.sh
```

Attach to 'finbench' container, and enter '/root/scripts'.
```
bash load_procedure.sh
```

Before benchmark, comment the line
```
# cd data && ln -s /root/datasets/sf100 ./sf100 && cd ..
```
in the file 'sf100_benchmark.properties'

Then run the benchmark.
```
bash sf100_benchmark.sh
```

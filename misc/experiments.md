# README

This README elaborates how to reproduce the experiments for choke-points analysis.

Note: These two experiments are conducted in docker.

Docker image: tugraph/tugraph-compile-centos8:1.3.1

# Temporal Locality in Storage

The temporal loacality experiment is executed on TuGraph-DB with FinBench SF100 dataset.

## Resources

- Package:
    
- Datasets:

## Experiment Steps

Execute following steps under current doc's directory, `ldbc_finbench_transaction_impls/misc`.

### Download Experiment Resources

#### Download Experiment Package
Download temporal locality experiment package into current directory.

```shell
$ wget {url of temporal locality experiment package}
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

#### Clone TuGraph-DB
clone the tugraph-db into experiment package directory，then checkout to finbench branch.

```shell
$ cd ./temporal_locality
$ git clone --recursive https://github.com/TuGraph-family/tugraph-db.git
$ cd ./tugraph-db
$ git checkout finbench
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

#### Compile TuGraph-DB

Compile tugraph-db inside docker container.

```shell
$ docker exec -it finbench_temporal_loaclity /bin/bash
$ cd /root/temporal_locality/tugraph-db
$ deps/build_deps.sh
$ mkdir build && cd build
$ cmake .. -DOURSYSTEM=centos -DENABLE_PREDOWNLOAD_DEPENDS_PACKAGE=1
$ make
$ make package
$ make install
```

use the following command to test if tugraph-db is successfully compile and installed.
```shell
$ lgraph_server -h
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
$ cd /root/temporal_locality/tugraph-db/build
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

Go to `ldbc_finbench_transaction_impls/tugraph` to run temporal locality benchmark

```shell
$ cd ../tugraph/
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

# Recursive Path Filtering

## Resources

- Pull Request:
- Package:
    - Baseline: 
    - Optimized:
- Datasets:

## Experiment Steps
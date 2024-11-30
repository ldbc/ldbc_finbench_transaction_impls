# README

This README elaborates how to reproduce the experiments for choke-points analysis.

Note: These two experiments are conducted in docker.

Docker image: tugraph/tugraph-compile-centos8:1.3.1

# Temporal Locality in Storage

## Resources

- Package:
    - Baseline: 
    - Optimized:
- Datasets:

## Experiment Steps



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

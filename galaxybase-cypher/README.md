# Galaxybase Module @ FinBench Transaction Reference Implementation

This module presents a Galaxybase implementation for the FinBench benchmark.

- [Galaxybase Official Website](https://www.galaxybase.com/)

## 1. Preparation

- Have the `galaxybase_ldbc_finbench.zip` file at your disposal and unzip the file.
- Ensure the data files for `sf1` and `sf10` are within the `galaxybase_ldbc_finbench/datagen`.

## 2. Installation & Data Loading

Navigate to the database directory: `cd database`

- **Obtaining the Package**

To initiate the process of acquiring the Galaxybase package, kindly reach out to our support team at support@createlink.com. After your request is submitted, our team will conduct a brief review. Once approved, they will guide you through the necessary steps to access and download the package. After you have received the package, place it in the current directory.

- **Package Extraction**

```bash
tar -zxf galaxybase.tar.gz
```

- **Environment Setup and Image Installation**

Install the Galaxybase foundational environment.

```bash
./galaxybase-*/bin/galaxybase-deploy install docker
./galaxybase-*/bin/galaxybase-deploy image install
```

- **Service Container Deployment**

Deploy the `admin` and `graph` service containers.

```shell
./galaxybase-*/bin/galaxybase-deploy build admin --home home
./galaxybase-*/bin/galaxybase-deploy build graph --home home
```

- **Validation & Start-Up**

Navigate to `http://ip:51314` on your web browser and log in using the credentials:

- Username: admin
- Password: admin

Add node with the following parameters:

- Node address：127.0.0.1
- Node number：0

Initiate the start-up process with the following settings:

- Initial memory(GB): 30G
- Maximum memory(GB): 30G
- Graph data cache(GB): 20G
- Externally unique identifier cache(GB): 5G
- Backup: 1

Acquire the verification code from the support team at support@createlink.com.

- **Data Transfer**

Move the data to the `home/graph/data` directory.

```shell
mv ../datagen/sf1/snapshot home/graph/data/sf1
```

- **Data Loading**

```shell
./galaxybase-*/bin/galaxybase-load -s json/schema_sf1.json -m json/mapping_sf1.json -g sf1
```

## 3. Benchmark Execution

Navigate to the implements directory: `cd implements`

- **Compilation**

```shell
git clone https://github.com/ldbc/ldbc_finbench_transaction_impls 
cd ldbc_finbench_transaction_impls
mvn install -DskipTests
cd galaxybase-cypher
```

- **Benchmark Run**

```shell
# Create validation for sf1
nohup sh sf1_finbench_create_validation.sh > console.log &

# Validate sf1
nohup sh sf1_finbench_validation_database.sh > console.log &

# Run benchmark for sf10
nohup sh sf10_finbench_benchmark.sh > console.log &
```

## 4. Verification

Navigate to the check directory: `cd check`

- **ACID Verification**

```shell
sh galaxybase-acid.sh
```

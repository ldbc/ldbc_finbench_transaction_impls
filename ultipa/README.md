# Ultipa Module @ FinBench Transaction Reference Implementation

This module presents a Ultipa implementation for the FinBench benchmark.

- https://www.ultipa.com/
- https://www.ultipa.cn/

# 1. Preparation

## 1.1  Get server & algo & transporter & license

To obtain the ultipa server package& algo & transporter & license, please contact our support team at [support@ultipa.com]. Upon submission of your request, our team will conduct a brief review. Once approved, they will guide you through the necessary steps to access and download the software package. After receiving the package, please place it in the current directory.

## 1.2 Set environment variables

```
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
export LD_LIBRARY_PATH=/usr/local/lib64:${JAVA_HOME}/jre/lib/amd64/server
export PATH=$PATH:/data/apache-maven-3.9.2/bin
export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8
```

## 1.3 Download & install dependencies

```
# install driver
cd /data
git clone https://github.com/ldbc/ldbc_finbench_driver.git
cd /data/ldbc_finbench_driver
mvn install

# install impl
cd /data
git clone https://github.com/ldbc/ldbc_finbench_transaction_impls.git
cd /data/ldbc_finbench_transaction_impls && git checkout ultipa
mvn clean package

# install acid
cd /data
git clone https://github.com/ldbc/ldbc_finbench_acid.git
mvn install
```

## 1.3  Install  UltipaServer

```
#create directory
mkdir -p /opt/ultipa-server
cd /opt/ultipa-server

# load server images
docker load -i [server_images.tar]

# modify tagname
docker tag [imageid] [imagename_new]:[tag_new]

#start container
docker run -itd --privileged --cap-add=SYS_PTRACE \
--security-opt seccomp=unconfined --net=host \
-e TZ=Asia/Shanghai \
-v /opt/ultipa-server/server/data:/opt/ultipa-server/data \
-v /opt/ultipa-server/server/config:/opt/ultipa-server/config \
-v /opt/ultipa-server/server/log:/opt/ultipa-server/log \
-v /opt/ultipa-server/server/exta:/opt/ultipa-server/exta \
--name $container_name [imagename_new]:[tag_new] 

docker exec -it $container_name  bash start

#copy lincese
cp -r ./ultipa.lic /opt/ultipa-server/server/resource/cert/

#copy algo
cp -r ./libexta_complex_read.so /opt/ultipa-server/server/exta/lib/
cp -r ./complex_read.xml /opt/ultipa-server/server/exta/config/

#modify config
vi /opt/ultipa-server/config/server.config
1.  -> host=host:60061 server_id=1
2.  -> private_add=host:60161
3.  -> conf=$host1:60161|$host1:60061:1

#restart 
docker restart $container_name

```

# 2. Loading and preprocessing

## 2.1 Data

```
/data/ldbc_finbench_transaction_impls/ultipa/data
├── sf1
│   ├── incremental
│   ├── read_params
│   └── snapshot
└── sf10
    ├── incremental
    ├── read_params
    └── snapshot
```

## 2.2  Data Transfer

```
sh /data/ldbc_finbench_transaction_impls/ultipa/scripts/transferData.sh
```

## 2.3 Load data

```
# use transporter load data
cd /data/ldbc_finbench_transaction_impls/ultipa/scripts/
./ultipa-importer --config import_data.yml
```

## 2.4  Load To Engine

```python 
python3 /data/ldbc_finbench_transaction_impls/ultipa/scripts/lte.py
```

# 3. Benchmark

## 3.1 Validate

### 3.1.1 Create validation

```
# create validation
nohup sh sf1_finbench_create_validation.sh > create_validation.log &
```

### 3.1.2 Validate

```
# validate
nohup sh sf1_finbench_validation_database.sh > validation_database.log &
```

# 4. ACID Tests

## 4.1 Compile & run ACID tests

ACID test implementations are reviewed for compliance with the ACID test specification and implement all specified test cases. Additionally, tests execute successfully without atomicity and isolation test failures, supporting serializable isolation level transaction settings.

```
cd /data/ldbc_finbench_acid/ultipa
mvn -Dtest=UltipaAcidTest test
```


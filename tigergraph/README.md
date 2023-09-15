# TigerGraph LDBC Financial Implemention

- [1. Preparation](#1-preparation)
  - [1.1 Install TigerGraph](#11-install-tigergraph)
  - [1.2 Download & Install Dependencies](#12-download--install-dependencies)
- [2. DDL & Loading & Queries](#2-ddl--loading--queries)
  - [2.1 Create schema and loading job](#21-create-schema-and-loading-job)
  - [2.2 Load data](#22-load-data)
    - [2.2.1 Data directory structure](#221-data-directory-structure)
    - [2.2.2 Data Loading](#222-data-loading)
  - [2.3 Create and install queries](#23-create-and-install-queries)
  - [2.4 One-step setup](#24-one-step-setup)
- [3. Validate](#3-validate)
  - [3.1 Create validation](#31-create-validation)
  - [3.2 Validate](#32-validate)

---

## 1. Preparation

Before using the project, follow these steps to prepare your environment.

### 1.1 Install TigerGraph

For detailed installation instructions, please refer to the official documentation: [TigerGraph Getting Started Guide](https://docs.tigergraph.com/tigergraph-server/current/getting-started/linux)

### 1.2 Download & Install Dependencies

To install the project dependencies, perform the following actions:

```bash
cd ~
git clone https://github.com/ldbc/ldbc_finbench_transaction_impls.git
cd ~/ldbc_finbench_transaction_impls
mvn clean package
```

## 2. DDL & Loading & Queries
Ensure that you have already installed TigerGraph and authenticated as a user with the necessary privileges to run TigerGraph.
### 2.1 Create schema and loading job
```bash
bash ~/ldbc_finbench_transaction_impls/tigergraph/scripts/setup_schema.sh
```

### 2.2 load data
#### 2.2.1 Data directory structure
```bash
$ tree -L 2 ~/ldbc_finbench_transaction_impls/tigergraph/data
├── sf1
│   ├── deletes
│   ├── incremental
│   ├── inserts
│   ├── raw
│   ├── read_params
│   └── snapshot
└── sf10
    ├── incremental
    ├── raw
    ├── read_params
    └── snapshot
    ...
```

#### 2.2.2 Data Loading
```bash
bash ~/ldbc_finbench_transaction_impls/tigergraph/scripts/load_data.sh
```

### 2.3 Create and install queries
```bash
bash ~/ldbc_finbench_transaction_impls/tigergraph/scripts/create_queries.sh
bash ~/ldbc_finbench_transaction_impls/tigergraph/scripts/install_queries.sh
```

### 2.4 one-step setup
If you prefer to avoid separately performing steps 2.1, 2.2, and 2.3, you can use the convenient ```one_step_env.sh``` script, which automates the setup process of creating schemas, loading data, and preparing queries.
```bash
bash ~/ldbc_finbench_transaction_impls/tigergraph/scripts/one_step_env.sh
```

## 3. Validate
### 3.1 Create validation
```shell
cd ~/ldbc_finbench_transaction_impls/tigergraph
bash run.sh create_validation.properties
```

### 3.2 Validate
```shell
cd ~/ldbc_finbench_transaction_impls/tigergraph
bash run.sh validate_database.properties
```
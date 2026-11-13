# ECE Big Data Processing - IMDb project

## Contributors

| Name | Git username | Email |
| --- | --- | --- |
| Arthur Perreaut | filou94 | <!-- TODO --> |
| Gilles Foucart | gillesfcr | <!-- TODO --> |

Group: gr-04

## Repository layout

```text
project/
├── compose.yaml        # Kafka + Jupyter/PySpark stack
├── jobs/               # PySpark scripts (mounted in /home/jovyan/jobs)
│   ├── common.py       # paths, Spark session, IMDb schemas
│   ├── download_imdb.py
│   └── ingest_bronze.py
├── notebooks/          # Jupyter notebooks (mounted in /home/jovyan/work)
├── data/               # not versioned (mounted in /home/jovyan/data)
│   ├── raw/            # IMDb .tsv.gz files
│   ├── bronze/         # raw tables as Parquet, all strings
│   ├── silver/         # cleaned and typed tables
│   └── gold/           # analytical outputs
├── PROJECT.md
└── REPORT.md
```

## Execution and operations guide

### Prerequisites

- Docker and Docker Compose v2
- At least 5 GB of RAM allocated to Docker (Docker Desktop: Settings >
  Resources). The Spark driver uses 3 GB by default; lower it with
  `SPARK_DRIVER_MEMORY=2g` if needed.
- About 15 GB of free disk space (raw files + Parquet layers)

### 1. Start the stack

```bash
cd project
docker compose up -d
docker compose logs pyspark | grep "127.0.0.1:8888"   # Jupyter URL + token
```

Jupyter is available at <http://localhost:8888> and the Spark UI at
<http://localhost:4040> while a job is running.

### 2. Download the IMDb dataset

```bash
docker exec -it project_pyspark python /home/jovyan/jobs/download_imdb.py
```

### 3. Ingest the raw files (bronze layer)

```bash
docker exec -it project_pyspark python /home/jovyan/jobs/ingest_bronze.py
```

A single table can be (re)ingested with
`ingest_bronze.py title.principals`.

### 4. Data preparation

<!-- TODO -->

### 5. Data exploration

<!-- TODO -->

### 6. Data enrichment

<!-- TODO -->

### 7. Streaming

<!-- TODO -->

### 8. Open question

<!-- TODO -->

### Stop the stack

```bash
docker compose down
```

## AI / LLM usage declaration

| Tool | Used for |
| --- | --- |
| <!-- TODO e.g. Claude (Anthropic) --> | <!-- TODO tasks --> |

AI tools were not used to write the explanations or reasoning in
`REPORT.md`.

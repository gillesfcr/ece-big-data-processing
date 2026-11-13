"""Shared helpers for the project: paths, Spark session and IMDb schemas.

Data layout (under DATA_DIR):
    raw/     original IMDb .tsv.gz files (downloaded as is)
    bronze/  1:1 copy of the TSVs as Parquet, every column kept as a string
    silver/  cleaned and typed tables (written by the data preparation step)
    gold/    analytical outputs (exploration, enrichment, open question)
"""

import os

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATA_DIR = os.environ.get("DATA_DIR", "/home/jovyan/data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")

IMDB_BASE_URL = "https://datasets.imdbws.com"

# IMDb marks missing values with the literal string "\N"
IMDB_NULL = "\\N"

# Typed schema of every IMDb table: column -> Spark type.
# Order matters, it is the column order of the TSV files.
# Array columns (genres, directors, ...) are kept as comma-separated
# strings here and split during data preparation.
IMDB_TABLES = {
    "title.basics": {
        "tconst": "string",
        "titleType": "string",
        "primaryTitle": "string",
        "originalTitle": "string",
        "isAdult": "int",
        "startYear": "int",
        "endYear": "int",
        "runtimeMinutes": "int",
        "genres": "string",
    },
    "title.akas": {
        "titleId": "string",
        "ordering": "int",
        "title": "string",
        "region": "string",
        "language": "string",
        "types": "string",
        "attributes": "string",
        "isOriginalTitle": "int",
    },
    "title.crew": {
        "tconst": "string",
        "directors": "string",
        "writers": "string",
    },
    "title.episode": {
        "tconst": "string",
        "parentTconst": "string",
        "seasonNumber": "int",
        "episodeNumber": "int",
    },
    "title.principals": {
        "tconst": "string",
        "ordering": "int",
        "nconst": "string",
        "category": "string",
        "job": "string",
        "characters": "string",
    },
    "title.ratings": {
        "tconst": "string",
        "averageRating": "double",
        "numVotes": "int",
    },
    "name.basics": {
        "nconst": "string",
        "primaryName": "string",
        "birthYear": "int",
        "deathYear": "int",
        "primaryProfession": "string",
        "knownForTitles": "string",
    },
}


def table_dir(table: str) -> str:
    """Folder name used for a table, e.g. 'title.basics' -> 'title_basics'."""
    return table.replace(".", "_")


def get_spark(app_name: str, with_kafka: bool = False) -> SparkSession:
    """Create (or reuse) a local SparkSession tuned for a small laptop."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory",
                os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
        # default is 200, far too many for a single machine
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if with_kafka:
        spark_version = pyspark.__version__
        scala = "2.13" if spark_version.startswith("4") else "2.12"
        builder = builder.config(
            "spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{spark_version}",
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze(spark: SparkSession, table: str) -> DataFrame:
    """Raw table, all columns as strings, "\\N" not converted."""
    return spark.read.parquet(os.path.join(BRONZE_DIR, table_dir(table)))


def read_silver(spark: SparkSession, table: str) -> DataFrame:
    """Cleaned table produced by the data preparation step."""
    return spark.read.parquet(os.path.join(SILVER_DIR, table_dir(table)))


def read_typed(spark: SparkSession, table: str) -> DataFrame:
    """Bronze table with "\\N"/empty -> null and columns cast to their type.

    Lightweight stand-in for the silver tables, so the exploration can start
    before the data preparation is finished. Swap to read_silver() at the end.
    """
    df = read_bronze(spark, table)
    cols = []
    for name, dtype in IMDB_TABLES[table].items():
        c = F.col(name)
        c = F.when((c == IMDB_NULL) | (F.trim(c) == ""), None).otherwise(c)
        cols.append(c.cast(dtype).alias(name))
    return df.select(*cols)

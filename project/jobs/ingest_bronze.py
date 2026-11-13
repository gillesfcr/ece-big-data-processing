"""Convert the raw IMDb TSV files into Parquet (bronze layer).

Usage (inside the pyspark container):
    python /home/jovyan/jobs/ingest_bronze.py [table ...]

Without arguments every table is converted. Every column is kept as a
string and "\\N" / empty values are NOT converted, so the data preparation
step can measure exactly how values are missing (\\N vs empty vs blank).

Files are read as plain text and split on tabs instead of using the CSV
reader: IMDb does not quote fields (titles may contain a lone '"') and
Spark's CSV reader turns empty fields into null. Rows that do not have the
expected number of fields are written to bronze/_malformed/<table>.
"""

import os
import sys
import time

from pyspark.sql import functions as F

from common import BRONZE_DIR, IMDB_TABLES, RAW_DIR, get_spark, table_dir

# Parquet files of at most this many rows. Avoids a full shuffle
# (repartition) while still producing several files per table.
MAX_RECORDS_PER_FILE = 5_000_000


def ingest(spark, table: str) -> None:
    src = os.path.join(RAW_DIR, f"{table}.tsv.gz")
    dest = os.path.join(BRONZE_DIR, table_dir(table))
    bad_dest = os.path.join(BRONZE_DIR, "_malformed", table_dir(table))
    columns = list(IMDB_TABLES[table])
    header = "\t".join(columns)

    start = time.time()
    lines = (
        spark.read.text(src)
        .filter(F.col("value") != header)
        # limit=-1 keeps trailing empty fields
        .withColumn("fields", F.split(F.col("value"), "\t", -1))
        .withColumn("n_fields", F.size("fields"))
    )
    good = lines.filter(F.col("n_fields") == len(columns)).select(
        *[F.col("fields")[i].alias(c) for i, c in enumerate(columns)]
    )
    bad = lines.filter(F.col("n_fields") != len(columns)).select(
        F.lit(table).alias("table"), "n_fields", F.col("value").alias("line")
    )

    (
        good.write.mode("overwrite")
        .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
        .parquet(dest)
    )
    bad.write.mode("overwrite").parquet(bad_dest)

    rows = spark.read.parquet(dest).count()
    n_bad = spark.read.parquet(bad_dest).count()
    print(f"[ok] {table}: {rows:,} rows, {n_bad:,} malformed "
          f"in {time.time() - start:.0f}s")


def main() -> None:
    tables = sys.argv[1:] or list(IMDB_TABLES)
    unknown = set(tables) - set(IMDB_TABLES)
    if unknown:
        sys.exit(f"unknown table(s): {', '.join(sorted(unknown))}")
    spark = get_spark("ingest_bronze")
    for table in tables:
        ingest(spark, table)
    spark.stop()


if __name__ == "__main__":
    main()

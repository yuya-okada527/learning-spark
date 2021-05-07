import sys

from pyspark.sql import SparkSession


def main(mnm_file: str) -> None:
    spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())

    # load
    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file)
    )

    # count
    count_mnm_df = (mnm_df
        .select("State", "Color", "Count")
        .groupBy("State", "Color")
        .sum("Count")
        .orderBy("sum(Count)", ascending=False)
    )

    # show
    count_mnm_df.show(n=60, truncate=False)

    ca_count_mnm_df = (mnm_df
        .select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .sum("Count")
        .orderBy("sum(Count)", ascending=False)
    )

    ca_count_mnm_df.show(n=10, truncate=False)
    spark.stop()









if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    main(sys.argv[1])
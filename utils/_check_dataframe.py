
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, DoubleType
import pandas as pd

def check_dataframe(dataset: DataFrame):
    """
    General information for Spark DataFrames, null values, unique values, 
    and statistical summary information.
    
    Args:
        dataset (DataFrame): Spark DataFrame to be analyzed.
    """
    print("-" * 10 + ' Shape Information of Dataset ' + "-" * 10, end='\n' * 2)
    row_count = dataset.count()
    col_count = len(dataset.columns)
    print(f" The dataset consist of {row_count} rows & {col_count} columns", end='\n' * 2)

    print("-" * 10 + ' General informations about to Dataset ' + "-" * 10, end='\n' * 2)
    print("Dataset Schema (Equivalent to pandas info()):")
    dataset.printSchema()
    print("-" * 10, end='\n' * 2)
    print("-" * 10 + " Are there any null values in the dataset? " + "-" * 10, end='\n' * 2)
    null_counts = dataset.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in dataset.columns]).collect()[0].asDict()
    sorted_null_counts = dict(sorted(null_counts.items(), key=lambda item: item[1], reverse=True))
    for col, count in sorted_null_counts.items():
        print(f"{col}: {count}")
    print("-" * 10, end='\n' * 2)
    print("-" * 10 + " Are there any dublicated values in the dataset? " + "-" * 10, end='\n' * 2)
    duplicate_count = row_count - dataset.dropDuplicates().count()
    print(duplicate_count, end='\n' * 2)
    print("-" * 10 + " What is the Number of Unique Classes in the Variables ? " + ' ' + "-" * 10, end='\n' * 2)
    unique_counts = {col: dataset.agg(F.countDistinct(F.col(col))).collect()[0][0] for col in dataset.columns}
    for col, count in unique_counts.items():
        print(f"{col}: {count}")
    print("-" * 10, end='\n' * 2)
    print('-' * 10 + ' Descriptive Statistics of Numerical Features ' + "-" * 10, end='\n' * 2)
    numerical_cols = [c.name for c in dataset.schema if c.dataType.typeName() in ('integer', 'long', 'float', 'double')]
    desc_df = dataset.describe(*numerical_cols).toPandas()
    desc_df.index = desc_df['summary']
    desc_df = desc_df.drop('summary', axis=1).T
    print(desc_df, end='\n' * 2)
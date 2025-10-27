from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import seaborn as sns
from pyspark.sql.types import ArrayType, DoubleType
import matplotlib.pyplot as plt
import pandas as pd

def num_summary(dataframe: DataFrame, numerical_cols: list = None, plot: bool = False):
    """
        PySpark calculates summary statistics for the specified numeric columns in the DataFrame and optionally plots a scatter plot.

        Args:
        dataframe (DataFrame): The PySpark DataFrame to be processed.
        numerical_cols (list): A list of numeric column names to be summarized. If None, an attempt is made to identify all numeric columns.
        plot (bool): Whether to plot a scatter plot (histogram)
    """

    if numerical_cols is None:
        numerical_cols = [c.name for c in dataset.schema if c.dataType.typeName() in ('integer', 'long', 'float', 'double')]

    quantiles = [0.25, 0.50, 0.75, 0.90, 0.95, 1.0]

    for col_name in numerical_cols:
        print("/"*10 + f" Summary Statistics Of {col_name} " + "*"*10)

        quantile_values = dataframe.approxQuantile(col_name, quantiles, 0.01)

        quantile_summary = {
            f'{int(q*100)}%': val 
            for q, val in zip(quantiles, quantile_values)
        }

        basic_stats_df = dataframe.select(col_name).describe().collect()

        basic_stats_summary = {
            row['summary']: float(row[col_name]) 
            for row in basic_stats_df if row['summary'] in ['count', 'mean', 'stddev', 'min', 'max']
        }

        final_summary = {
            'count': basic_stats_summary.get('count'),
            'mean': basic_stats_summary.get('mean'),
            'std': basic_stats_summary.get('stddev'),
            'min': basic_stats_summary.get('min'),
            '25%': quantile_summary.get('25%'),
            '50%': quantile_summary.get('50%'),
            '75%': quantile_summary.get('75%'),
            '90%': quantile_summary.get('90%'),
            '95%': quantile_summary.get('95%'),
            'max': basic_stats_summary.get('max'),
        }

        summary_series = pd.Series(final_summary)
        print(summary_series.to_string())

        if plot:
            try:
                pd_df = dataframe.select(col_name).toPandas()
            except Exception as e:
                print(f"\nWARNING: Error converting data to Pandas for visualization:{e}")
                print("Your dataset may be too large. Visualization is being skipped.")
                continue

        print("\n" + "-"*10 + f" Plotting Distribution Of {col_name} " + "-"*10)

        plt.figure(figsize=(8, 5))
        sns.histplot(data=pd_df, x=col_name, kde=True)
        plt.xlabel(col_name)
        plt.ylabel("Density")
        plt.title("The Distribution of " + col_name)
        plt.grid(True)
        plt.show(block=True)
        print("\n")




from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import seaborn as sns
import pandas as pd


def desc_stats(dataframe: DataFrame, numerical_cols: list):
    """
    Calculates the statistical summary of the Spark DataFrame (describe()) and
    visualizes the output using a heatmap.
    
    Args:
        dataframe (DataFrame): The Spark DataFrame to be analyzed.
        numerical_cols (list): A list of numerical column names to describe.
    """
    print("-" * 10 + ' Descriptive Statistics & Visualization ' + "-" * 10, end='\n'*2)
    desc_df = dataframe.describe(*numerical_cols)
    quantiles_list = [0.25, 0.50, 0.75]
    quantile_names = [f'{int(q*100)}%' for q in quantiles_list]
    quantiles_data = {}
    relative_error = 0.01

    for col in numerical_cols:
        try:
            #quantiles = dataframe.approximateQuantile(col, quantiles_list, relativeError=relative_error)
            #quantiles_data[col] = quantiles
            quantile_exprs = [F.expr(f"percentile_approx({col}, {q})").alias(f"{int(q*100)}%") for q in quantiles_list]

            results = dataframe.select(*quantile_exprs).collect()[0]
            quantiles_data[col] = [results[i] for i in range(len(quantiles_list))]
        except Exception as e:
            print(f"Warning: Could not calculate quantiles for {col}. Error: {e}")
            quantiles_data[col] = [None] * len(quantiles_list)

    desc_pd = desc_df.toPandas()
    desc_pd.index = desc_pd['summary']
    desc_pd = desc_pd.drop('summary', axis=1).T

    quantiles_pd = pd.DataFrame(quantiles_data, index=quantile_names).T
    final_desc_pd = pd.concat([desc_pd, quantiles_pd], axis=1)

    ordered_cols = ['count',  'mean', 'stddev', 'min', '25%', '50%', '75%', 'max']
    final_cols = [col for col in ordered_cols if col in final_desc_pd.columns]

    for col in final_desc_pd.columns:
        if col not in final_cols:
            final_cols.append(col)
    final_desc_pd = final_desc_pd[final_cols]

    for col in final_desc_pd.columns:
        try:
            final_desc_pd[col] = pd.to_numeric(final_desc_pd[col], errors='coerce')
        except:
            pass

    f, ax = plt.subplots(figsize=(15, 
                                 desc_pd.shape[0] * 0.78))   
    sns.heatmap(final_desc_pd,
                annot=True,
                cmap="Wistia",
                fmt='.2f',
                ax=ax,
                linecolor='white',
                linewidths=1.3,
                cbar=False,
                annot_kws={"size": 12})
    plt.xticks(size=18)
    plt.yticks(size=14,
               rotation=0)
    plt.title("Descriptive Statistics", size=14)
    plt.show()
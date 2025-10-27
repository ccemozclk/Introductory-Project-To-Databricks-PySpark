from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from typing import Literal

AggFunc = Literal["mean", "median", "min", "max", "sum"]


def calculate_aggregation(dataframe: DataFrame, categorical_column: str, numerical_column: str, agg_func: AggFunc, top_n: int) -> pd.DataFrame:
    """
    Calculates the aggregation (mean, median, etc.) of the numerical column 
    by the categorical column entirely within PySpark and returns a small Pandas DataFrame.
    """

    print("\n" + "#" * 10 + " Calculating Aggregation Data " + "#" * 10)
    print(f"Aggregation: {categorical_column} vs {numerical_column} (Function: {agg_func.upper()})")

    top_categories_df = dataframe.groupBy(categorical_column).count()
    
    top_categories_pd = (top_categories_df.orderBy(F.col("count").desc()).limit(top_n).select(categorical_column).toPandas() )
    
    top_categories_list = top_categories_pd[categorical_column].tolist()

    if not top_categories_list:
        print(f"Warning: No categories found in {categorical_column} to analyze.")
        return pd.DataFrame()

    print(f"Top {top_n} categories by frequency: {top_categories_list}")

    filtered_df = dataframe.filter(F.col(categorical_column).isin(top_categories_list))

    if agg_func == "mean":
        agg_col = F.mean(F.col(numerical_column)).alias("Aggregated_Value")
        
    elif agg_func == "min":
        agg_col = F.min(F.col(numerical_column)).alias("Aggregated_Value")
        
    elif agg_func == "max":
        agg_col = F.max(F.col(numerical_column)).alias("Aggregated_Value")
        
    elif agg_func == "sum":
        agg_col = F.sum(F.col(numerical_column)).alias("Aggregated_Value")
        
    elif agg_func == "median":
        agg_col = F.expr(f"percentile_approx({numerical_column}, 0.5)").alias("Aggregated_Value")

    else:
        raise ValueError(f"Unsupported aggregation function: {agg_func}. Use 'mean', 'median', 'min', 'max', or 'sum'.")

    agg_df = (filtered_df.groupBy(categorical_column).agg(agg_col))

    pd_agg_data = agg_df.toPandas()
    
    return pd_agg_data, top_categories_list, filtered_df



def cat_with_numerical(dataframe: DataFrame, categorical_column: str, numerical_column: str, agg_func: AggFunc,top_n: int = 10):
    print("\n" + "#" * 10 + " Categorical-Numerical Interaction Analysis " + "#" * 10)
    print(f"Visualizing: {categorical_column} vs {numerical_column} (Aggregation: {agg_func.upper()})")

    agg_data, top_categories_list, filtered_df = calculate_aggregation(dataframe,categorical_column, numerical_column,  agg_func, top_n )
    
    if agg_data.empty:
        return

    pd_plot_df = filtered_df.select(categorical_column, numerical_column).toPandas() 
    pd_plot_df.dropna(subset=[categorical_column, numerical_column], inplace=True)

    category_order = top_categories_list

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(1, 2, figsize=(18, 7))

    sns.boxplot( x=categorical_column, y=numerical_column, data=pd_plot_df, order=category_order, ax=axes[0],palette="pastel", hue=categorical_column, legend=False)

    axes[0].set_title(f'Distribution of {numerical_column} by {categorical_column}', fontsize=14)
    axes[0].set_xticks(axes[0].get_xticks()) 
    axes[0].set_xticklabels(axes[0].get_xticklabels(), rotation=45, ha='right')
    axes[0].set_xlabel(categorical_column, fontsize=14)
    axes[0].set_ylabel(numerical_column, fontsize=14)

    sns.pointplot(x=categorical_column, y="Aggregated_Value", data=agg_data, order=category_order, ax=axes[1], color='darkred', linestyle='-',errorbar=None )

    agg_data_sorted = agg_data.set_index(categorical_column).reindex(category_order).reset_index()
    for i, row in agg_data_sorted.iterrows():
        axes[1].text(i, row["Aggregated_Value"], f'{row["Aggregated_Value"]:.2f}', ha='center', va='bottom',fontsize=14,color='darkred')

    axes[1].set_title(f'{agg_func.upper()} of {numerical_column} by {categorical_column}', fontsize=14)
    
    axes[1].set_xlabel(categorical_column, fontsize=14)
    axes[1].set_ylabel(f'{agg_func.upper()} {numerical_column}', fontsize=14)

    axes[1].set_xticks(axes[1].get_xticks())
    axes[1].set_xticklabels(axes[1].get_xticklabels(), rotation=45, ha='right') 

    min_val = agg_data['Aggregated_Value'].min()
    max_val = agg_data['Aggregated_Value'].max()
    axes[1].set_ylim(bottom=min_val * 0.99, top=max_val * 1.01)
    
    plt.tight_layout()
    plt.show()


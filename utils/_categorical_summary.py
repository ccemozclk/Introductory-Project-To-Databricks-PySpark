from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd




def cat_summary(dataframe: DataFrame, cat_cols: list, plot: bool = False, top_n: int = 10,hue: str = None):
    """
    Calculates the number of unique values, frequency, and ratios for categorical columns in a Spark DataFrame. Optionally visualizes the results.
    
    Args:
        dataframe (DataFrame): The Spark DataFrame to be analyzed.
        cat_cols (list): A list of categorical column names.
        plot (bool): Whether to plot the frequency graph.
        top_n (int): The number of top categories to display and plot.
        hue (str, optional): A column name for the 'hue' parameter in the seaborn barplot for sub-group visualization. Defaults to None.
    """
    total_row_count = dataframe.count()

    for col_name in cat_cols:
        print("\n" + "#" * 10 + " Unique Observations of Categorical Data " + "#" * 10)
        unique_count = dataframe.select(F.col(col_name).cast("string")).agg(
             F.countDistinct(F.col(col_name))
         ).collect()[0][0]

        print(f"The unique number of {col_name}: {unique_count}")
        print("\n" + "#" * 10 + " Frequency of Categorical Data " + "#" * 10)
        #freq_df = dataframe.groupBy(F.col(col_name)).count().orderBy(F.desc("count"))
        summary_df = (
            dataframe.groupBy(F.col(col_name).cast("string"))
            .count()
            .withColumnRenamed("count", "Frequency")
        )
        summary_df = summary_df.withColumnRenamed(summary_df.columns[0], col_name)
        summary_df = summary_df.withColumn(
            "Ratio_Numeric", 
            F.col("Frequency") / F.lit(total_row_count)
        )
        summary_df = summary_df.withColumn(
            "Ratio",
            F.concat(
                F.round(F.col("Ratio_Numeric") * 100, 2), 
                F.lit("%")
            ).cast("string")
        )
        summary_df = summary_df.orderBy(F.col("Ratio_Numeric").desc()).limit(top_n)
        #summary_df = summary_df.select(col_name, "Frequency", "Ratio")

        pd_summary_df = summary_df.toPandas()
        pd_summary_df.dropna(subset=[col_name], inplace=True)
        print(pd_summary_df.set_index(col_name))

        if plot:
            print(f"\n" + "#" * 10 + f" Plotting Top {top_n}: {col_name} " + "#" * 10)

            if hue and hue in dataframe.columns:
                print(f"Sub-group analysis (hue) for {col_name} by {hue}.")
                top_n_categories = pd_summary_df[col_name].tolist()
                plot_df = dataframe.filter(F.col(col_name).isin(top_n_categories)).groupBy(col_name, hue).count().withColumnRenamed("count", "Frequency")
                pd_plot_df = plot_df.toPandas()
                pd_plot_df.dropna(subset=[col_name], inplace=True)

                
                sns.set_theme(style="whitegrid", palette="deep")
                plt.figure(figsize=(12, 7))
                ax = sns.barplot(
                    x=col_name, 
                    y="Frequency", 
                    data=pd_plot_df, 
                    hue=hue,
                    order=top_n_categories 
                )
                plt.title(f'Top {top_n} Frequency Distribution of {col_name} by {hue}')
                
                

            else:
                sns.set_theme(style="darkgrid")
                rgb_values = sns.color_palette("Set2", len(pd_summary_df))
                category_order = pd_summary_df[col_name].tolist()
                plt.figure(figsize=(10, 6))
                ax = sns.barplot(
                    x=col_name, 
                    y="Frequency", 
                    data=pd_summary_df, 
                    hue=col_name,
                    palette=rgb_values,
                    order=category_order,
                    legend=False
                )
                plt.title(f'Top {top_n} Frequency Distribution of {col_name}')
                plt.xticks(rotation=45, ha='right')
                for i,p in enumerate(ax.patches):
                    ratio_str = pd_summary_df['Ratio'].iloc[i]
                    
                    ax.annotate(
                        ratio_str, 
                        (p.get_x() + p.get_width() / 2., p.get_height()),
                        ha='center', va='top', color='white', size=10,
                        xytext=(0, -20), textcoords='offset points'
                                )
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.show()
                
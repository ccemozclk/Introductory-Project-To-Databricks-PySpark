# Introductory Data Exploration Project with PySpark and Databricks

## 🎯 Project Goal

This project serves as an introductory exploratory data analysis (EDA) using the **New York City Taxi (NYCTaxi) dataset**. The main objective was to reinforce and apply the fundamental concepts of **PySpark** and **Databricks**, which I acquired from the DataCamp courses listed below. The project focuses on leveraging the scalable computing power of PySpark within the Databricks environment to handle and analyze large datasets efficiently.

## 📚 Learning Resources

This project was developed by applying the knowledge gained from the following DataCamp courses:

* **Introduction to Databricks:**
    * *Link:* [https://app.datacamp.com/learn/courses/introduction-to-databricks](https://app.datacamp.com/learn/courses/introduction-to-databricks)
* **Foundations of PySpark:**
    * *Link:* [https://app.datacamp.com/learn/courses/foundations-of-pyspark](https://app.datacamp.com/learn/courses/foundations-of-pyspark)

## 🛠️ Technologies and Tools

| Category | Tool / Library | Purpose |
| :--- | :--- | :--- |
| **Platform** | Databricks Workspace | Cloud-based environment for collaborative data science and engineering. |
| **Compute** | Apache Spark (via PySpark) | Scalable engine for processing large datasets (creating DataFrames, transformations). |
| **Dataset** | NYCTaxi Dataset | Primary dataset used for all analyses. |
| **Programming** | Python | Primary language for analysis. |
| **Data Analysis** | PySpark, Pandas | Core libraries for data manipulation and analysis. |
| **Visualization** | Matplotlib, Seaborn (or Databricks built-in plots) | Used for traditional Python-based data visualization alongside Spark-based analysis. |

## 📊 Project Highlights (Key Analysis Areas)

The project includes an exploratory analysis of the NYCTaxi data focusing on aspects such as:

* **Data Loading and Inspection:** Efficiently loading the large taxi dataset into Spark DataFrames within Databricks.
* **Data Cleaning:** Handling null values, filtering outliers, and performing basic data type conversions using Spark APIs.
* **Trip Analysis:** Analyzing trip duration, distance, and passenger counts.
* **Fare Analysis:** Exploring the distribution of total fare amounts and tip percentages.
* **Time-Based Analysis:** Examining travel patterns based on time of day, day of the week, or month.

## 📝 How to Use This Project

To run this project, you will need access to a Databricks workspace and the necessary NYCTaxi data (often available as a public dataset within Databricks).

1.  Clone this repository to your Databricks Workspace using the **Git Folders (Repos)** feature.
2.  Open the main notebook (e.g., `main_notebook.ipynb` or similar).
3.  Ensure your Databricks cluster is attached and running a Spark runtime environment.
4.  Execute the cells sequentially to reproduce the analysis.

---
**Author:** [Cem OZCELİK/ccemozclk]

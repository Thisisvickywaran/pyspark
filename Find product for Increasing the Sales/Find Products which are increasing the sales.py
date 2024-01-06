from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, min , lag, max

spark=SparkSession.builder\
    .master("local")\
    .appName("Find Product for Increasing the Sales")\
    .getOrCreate()


df_sales=spark.read.format("csv").option("header","true").load("sales_table.csv")
df_sales.show()

df_product=spark.read.format("csv").option("header","true").load("product_table.csv")
df_product.show()

df_previous_year_sales=df_sales.withColumn("previous_year_sales",lag(col("total_year_sales")).over(Window.partitionBy("product_id").orderBy("year")))

df_previous_year_sales.show()

df_diff=df_previous_year_sales.withColumn("Difference_sales",col("total_year_sales")-col("previous_year_sales"))

df_diff.show()

df_increase=df_diff.groupby("product_id").agg((min(col("Difference_sales"))).alias("Diff"))
df_increase.show()

df_increase_result=df_increase.filter(col("Diff")>0)

df_increase_result.show()

df_final_result=df_increase_result.join(df_product,on="product_id",how="inner")

df_final_result.show()


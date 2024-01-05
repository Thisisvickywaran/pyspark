from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Flatten_json") \
    .getOrCreate()


#import_json_file
df_json=spark.read.format("json").option("multiline",True).load("sample.json") #adding option multiline since multiline json
df_json.show(truncate=False) #we dont want the truncate so making it truncate false

df_exploded =df_json.select("Course_type","Head_Office_Contact","Institute_Name",explode(col("branches")).alias("branch_data"))
df_exploded.show()

df_exploded_struct=df_exploded.select("Course_type","Head_Office_Contact","Institute_Name","branch_data.*")
df_exploded_struct.show()
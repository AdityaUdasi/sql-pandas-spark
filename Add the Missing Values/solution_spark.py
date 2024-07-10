from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window


# Create a Spark session
spark = SparkSession.builder \
    .appName("Create Spark DataFrame Example") \
    .getOrCreate()

# Provided data
data = {
    'row_id': [x for x in range(1, 13)],
    'job_role': ['Data Engineer', None, None, None, None, 'Web Developer', None, None, 'Data Scientist', None, None, None],
    'skills': ['SQL', 'Python', 'AWS', 'Snowflake', 'Apache Spark', 'Java', 'HTML', 'CSS', 'Python', 'Machine Learning', 'Deep Learning', 'Tableau']
}

# Define schema for DataFrame
schema = StructType([
    StructField('row_id', IntegerType(), True),
    StructField('job_role', StringType(), True),
    StructField('skills', StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame([(data['row_id'][i], data['job_role'][i], data['skills'][i]) for i in range(len(data['row_id']))], schema=schema)

df_filled = df.withColumn('job_role', F.last('job_role', True).over(Window.orderBy('row_id').rowsBetween(Window.unboundedPreceding, 0)))

df_filled.show()
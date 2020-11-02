"""
PROBLEM STATEMENT - Get daily product revenue
"""

from pyspark.sql import SparkSession

# Initialize spark session
spark = SparkSession.builder.master('local').appName('CSV Example').getOrCreate()

orders = spark.read.foramt('csv').schema(
	'order_id int, order_date string, order_customer_id int, order_status string').load('/home/cloudera/data/retail_db/orders')

orders.printSchema()

	

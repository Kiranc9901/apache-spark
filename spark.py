df = spark.read.csv("dbfs:/FileStore/shared_uploads/kiranc9901@gmail.com/Order_s5aZL68ifM_4SdQ5AaMkl.csv", header=True, inferSchema=True, sep =',')
df.printSchema()
df.show()

#databricks function

display(df)

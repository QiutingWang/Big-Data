PySparkSQL and DataFrames

##For more reference guide:https://spark.apache.org/docs/latest/sql-getting-started.html

###Abstracting Data with DataFrame###
##PySpark Dataframe:
#immutable distributed collection of data with named columns
#support both SQL queries or expression methods(df.select())
#for processing both structured and semi-stuctured data

##Spark Session-Entry point for dataframe API:
#used to create,register,execute SQL queries
#SparkSession is available in PySpark shell as spark

##Creating DataFrames in PySpark:2 methods
#1.from existing RDDs using SparkSession's createDataFrame()
#2.from various data sources(CSV,TXT...) using SparkSession read method

##Schema:架构
#control data and help df to optimize queries
#provide information about column name/type of data in the column/empty value...

#create a DataFrame from RDD:
iphones_RDD = sc.parallelize([        #使用SparkContent并行法 创建RDD
    ("XS", 2018, 5.65, 2.79, 6.24),
    ("XR", 2018, 5.94, 2.98, 6.84),
    ("X10", 2017, 5.65, 2.79, 6.13),
    ("8Plus", 2017, 6.23, 3.07, 7.12)])
names = ['Model', 'Year', 'Height', 'Width', 'Weight']        #设置架构，the list of column name;如果并没有state出明确的schema，就使用推断infer的方法
iphones_df = spark.createDataFrame(iphones_RDD, schema=names) #使用sparksession中createDataFrame()语句
type(iphones_df)
#return:
yspark.sql.dataframe.DataFrame

#create a DataFrame from reading a CSV/TXT/JSON:
df_csv = spark.read.csv("people.csv", header=True, inferSchema=True)
df_json = spark.read.json("people.json", header=True, inferSchema=True)
df_txt = spark.read.txt("people.txt", header=True, inferSchema=True)
#header and inferSchema are optional parameters

###operating on DataFrame in PySpark###similar to RDD
##DataFrame operations=Transformation+Actions
#Transformations:
select(),filter(),groupby(),orderby(),dropDuplicates(),withColumnRenamed()
#Actions:
printSchema(),head(),show(),count(),coulmns and describe()

#select() and show()
df_id_age = test.select('Age')
df_id_age.show(3) #show() defaults to print first 20 columns
+---+
|Age|
+---+
| 17|
| 17|
| 17|
+---+
only showing top 3 rows

#filter():based on some conditions
new_df_age21 = new_df.filter(new_df.Age > 21)
new_df_age21.show(3) 
-----+---+-----+---+
|User_ID|Gender|Age|
+-------+------+---+
|1000002|M.    |55 |
|1000003|M.    |26 |
|1000004|M.    |46 |
+-------+------+---+
only showing top 3 rows

#groupby() and count():
est_df_age_group = test_df.groupby('Age')
test_df_age_group.count().show(3)
+---+------+
|Age| count|
+---+------+ 
| 26|219587|
| 17|     4|
| 55| 21504| 
only showing top 3 rows

#orderby():
test_df_age_group.count().orderBy('Age').show(3)

#dropDuplicates():
test_df_no_dup = test_df.select('User_ID','Gender', 'Age').dropDuplicates()
test_df_no_dup.count()
#return:5892

#withColumnRenamed:rename a column
test_df_sex = test_df.withColumnRenamed('Gender', 'Sex')
test_df_sex.show(3)
+-------+---+---+
|User_ID|Sex|Age|
+-------+---+---+
|1000001|  F| 17|
|1000001|  F| 17|
|1000001|  F| 17|
+-------+---+---+
 
#printSchema:print the type of columns
test_df.printSchema()
 |-- User_ID: integer (nullable = true)
 |-- Product_ID: string (nullable = true)
 |-- Gender: string (nullable = true)
 |-- Age: string (nullable = true)
 |-- Occupation: integer (nullable = true)
 |-- Purchase: integer (nullable = true)

#column:print the column
test_df.columns
['User_ID', 'Gender', 'Age']
 
#descibe:summary statistics
test_df.describe().show()
+-------+------------------+------+------------------+
|summary|           User_ID|Gender|               Age|
+-------+------------------+------+------------------+
|  count|            550068|550068|            550068|
|   mean|1003028.8424013031|  null|30.382052764385495|
| stddev|1727.5915855307312|  null|11.866105189533554|
|    min|           1000001|     F|                 0|
|    max|           1006040|     M|                55|
+-------+------------------+------+------------------+
 
###Interacting with PySpark with SQL Queries###
#execute sql queries:.sql()
df.createOrReplaceTempView("table1")  #Create a temporary table
df2 = spark.sql("SELECT field1, field2 FROM table1")
df2.collect()
[Row(f1=1, f2='row1'), Row(f1=2, f2='row2'), Row(f1=3, f2='row3')]

#extract data
test_df.createOrReplaceTempView("test_table")
query = '''SELECT Product_ID FROM test_table'''
test_product_df = spark.sql(query)
test_product_df.show(5)
+----------+
|Product_ID|
+----------+
| P00069042|
| P00248942|
| P00087842|
| P00085442|
| P00285442|
+----------+

#summarize and group data
test_df.createOrReplaceTempView("test_table")
query = '''SELECT Age, max(Purchase) FROM test_table GROUP BY Age'''
spark.sql(query).show(5)

#filtering columns
test_df.createOrReplaceTempView("test_table")
query = '''SELECT Age, Purchase, Gender FROM test_table WHERE Purchase > 20000 AND Gender == "F"'''
spark.sql(query).show(5)

###data visualization###
#matplotlib seaborn bokeh can not be used with PySpark
##plotting graphs three method:
#pyspark_dist_explore library
#toPandas()
#HandySpark library

#Pyspark_dist_explore:three functions-hist(),distplot(),pandas_histogram()
test_df = spark.read.csv("test.csv", header=True, inferSchema=True)
test_df_age = test_df.select('Age')
hist(test_df_age, bins=20, color="red")
#pandas:受限制，适合小size工作
test_df = spark.read.csv("test.csv", header=True, inferSchema=True)
test_df_sample_pandas = test_df.toPandas()  #Convert to Pandas DataFrame
test_df_sample_pandas.hist('Age')
#handySpark
test_df = spark.read.csv('test.csv', header=True, inferSchema=True)
hdf = test_df.toHandy()
hdf.cols["Age"].hist()

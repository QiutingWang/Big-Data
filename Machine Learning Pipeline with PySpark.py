####Machine Learning Pipeline with PySpark####
##pyspark.ml:Transformer and Estimator

##Data Type:String to integer##
#Spark requires numeric data for modeling
#use .cast() convert columns from your dataframe model_data to integer
dataframe = dataframe.withColumn("col", dataframe.col.cast("new_type"))
#EG:
#Create is_late
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)
#Convert to an integer,boolean columns can easily be converted to integers
model_data = model_data.withColumn("label", model_data.is_late.cast('integer'))
#Remove missing values
model_data = model_data.filter("arr_delay is not NULL and dep_delay is not NULL and air_time is not NULL and plane_year is not NULL")

##String and Factors##
#These are coded as strings and there isn't any obvious way to convert them to a numeric data type.
#pyspark.ml.features,encode your categorical feature
#1.create a StringIndexer containing estimators to map the unique string to a number,returns a transformer attaches the mapping to it as metadata,returns a new DataFrame with a numeric column
#2.OneHotEncoder:similar with StringIndexer, fitted for machine learning routines.

##For carrier and destimation:
# Create a StringIndexer
carr_indexer = StringIndexer(inputCol="carrier",outputCol="carrier_index")    #inputCol:the name of the column you want to index or encode;output:the name of the new column that the Transformer should create.
# Create a OneHotEncoder,将定性数据编码为定量数据
carr_encoder = OneHotEncoder(inputCol="carrier_index",outputCol="carrier_fact")

##Assemble a Vector##
#Pipeline combines all of the columns containing our features we have created into a single column.We can reuse the same modeling process over and over again.
#Every observation is a vector with a label
vec_assembler = VectorAssembler(inputCols=["month", "air_time", "carrier_fact", "dest_fact", "plane_age"], outputCol="features")
#create a pipeline:
# Import Pipeline
from pyspark.ml import Pipeline
# Make the pipeline
flights_pipe = Pipeline(stages=[dest_indexer, dest_encoder, carr_indexer, carr_encoder, vec_assembler])
#Stages should be a list holding all the stages you want your data to go through in the pipeline

##Test and Train##
#split the data into a test set and a train set. 
# Fit and transform the data
piped_data = flights_pipe.fit(model_data).transform(model_data)
# Split the data into training and test sets, training with 60% of the data and test with 40% of the data.
training, test = piped_data.randomSplit([.6, .4])
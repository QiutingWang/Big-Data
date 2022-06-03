PySpark MLib 

###Overview of PySpark MLib ###
##Advantage:
#popular libaray for data mining and machine learning
#sklearn only work for small datasets
#parallel processing on a cluster
#provide high-level api to build machine learning pipelines

##3C's of machine learning in PySpark MLib:
#collaborative filtering(recommendations)
#Classification 
#Clustering

##imports MLlib:
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.clustering import KMeans

###collaborative filtering###
##Definition:find users that share common interests
##Approach:
#User-User Collabortive filtering:finds users that are similar to target user
#Item-Item Collabortive filtering:finds and recommends items that are similar to items with the target user

##rating class in pyspark.mllib.recommendation submodule:
#wrapper around tuple(user,product,rating)
#useful for parsing RDD and creating a tuple of user,product,rating
#Create the rating
from pyspark.mllib.recommendation import Rating
r = Rating(user = 1, product = 2, rating = 5.0)
(r[0], r[1], r[2])
#result:(1, 2, 5.0)

##Splitting the data using randomSplit():randomly split with weights and return multiple RDDs
data = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
training, test=data.randomSplit([0.6, 0.4])
training.collect()
test.collect()
#return:
[1, 2, 5, 6, 9, 10]
[3, 4, 7, 8]

##Alternative Least Square(ALS)##交替最小二乘->ALS.train(ratings, rank, iterations),rank:# of features;iterations:运行最小二乘计算的迭代次数
r1 = Rating(1, 1, 1.0)
r2 = Rating(1, 2, 2.0)
r3 = Rating(2, 1, 2.0)
ratings = sc.parallelize([r1, r2, r3]) #create a RDD
ratings.collect() #print out the contents of RDD
#return:
[Rating(user=1, product=1, rating=1.0),
 Rating(user=1, product=2, rating=2.0),
 Rating(user=2, product=1, rating=2.0)]

model = ALS.train(ratings, rank=10, iterations=10)

##predictAll()-return RDD of Rating Objects##
#returns a list of predicted ratings for input user and product pair.Then returns a prediction.
unrated_RDD = sc.parallelize([(1, 2), (1, 1)])
predictions = model.predictAll(unrated_RDD)
predictions.collect()
#return:
[Rating(user=1, product=1, rating=1.0000278574351853),
 Rating(user=1, product=2, rating=1.9890355703778122)]
  
##Evaluation Use MSE##MSE:average square of (actual rating-predicted rating)
rates = ratings.map(lambda x: ((x[0], x[1]), x[2]))
rates.collect()
#return:[((1, 1), 1.0), ((1, 2), 2.0), ((2, 1), 2.0)]
preds = predictions.map(lambda x: ((x[0], x[1]), x[2]))
preds.collect()
#return:[((1, 1), 1.0000278574351853), ((1, 2), 1.9890355703778122)]
rates_preds = rates.join(preds)
rates_preds.collect()
#return:[((1, 2), (2.0, 1.9890355703778122)), ((1, 1), (1.0, 1.0000278574351853))]
MSE = rates_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean()

###Classification###
# PySpark MLib support binary classification and multiclass classification
# PySpark MLib contains a few specific data types:vectors,LabelledPoint

##vectors:2 flavors
#Dense Vector:store all their entries in an array of floating point numbers
denseVec = Vectors.dense([1.0, 2.0, 3.0])
DenseVector([1.0, 2.0, 3.0])
#Sparse Vector:store only the nonzero values and indices
sparseVec = Vectors.sparse(4, {1: 1.0, 3: 5.5})
SparseVector(4, {1: 1.0, 3: 5.5})

##labellpoint():wrapper for input features and predicted value
#Feature:for binary classification of logistic regression,0 (negative) or 1(positive)
positive = LabeledPoint(1.0, [1.0, 0.0, 3.0])
negative = LabeledPoint(0.0, [2.0, 1.0, 1.0])
print(positive)
print(negative)
#return:
LabeledPoint(1.0, [1.0,0.0,3.0])
LabeledPoint(0.0, [2.0,1.0,1.0])

##HashingTF():used to map feature value to indices in the feature vector散列.compute a term frequency vector of a given size from a document
from pyspark.mllib.feature import HashingTF
sentence = "hello hello world"
words = sentence.split() #splite into a list of words
tf = HashingTF(10000) #vector of size=10000
tf.transform(words) #compute the frequency vector
#return: a sparse vector with feature number and occurrence of each word
SparseVector(10000, {3065: 1.0, 6861: 2.0}) 
 
##LogisticRegressionWithLBFGS():
#to achieve logistic regression using PySpark MLlib
#the minimum requirement for it is an RDD of LabeledPoint
data = [                                 #we create a list of LabelPoint with labels 0 and 1
        LabeledPoint(0.0, [0.0, 1.0]),
        LabeledPoint(1.0, [1.0, 0.0]),]
RDD = sc.parallelize(data)              #create needed RDD
lrm = LogisticRegressionWithLBFGS.train(RDD)
lrm.predict([1.0, 0.0])
lrm.predict([0.0, 1.0])
#return: 1 0

###Introduction to Clustering###
##K-means:
RDD = sc.textFile("WineData.csv"). \
       map(lambda x: x.split(",")).\
       map(lambda x: [float(x[0]), float(x[1])])
RDD.take(5)
#return:[[14.23, 2.43], [13.2, 2.14], [13.16, 2.67], [14.37, 2.5], [13.24, 2.87]]

#model training:
from pyspark.mllib.clustering import KMeans
model = KMeans.train(RDD, k = 2, maxIterations = 10)
model.clusterCenters
#return:[array([12.25573171,  2.28939024]), array([13.636875  ,  2.43239583])]

#evluating:compute error function
from math import sqrt
def error(point):
    center = model.centers[model.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))
WSSSE = RDD.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))
#return: Within Set Sum of Squared Error = 77.96236420499056

#Visualizing:
wine_data_df = spark.createDataFrame(RDD, schema=["col1", "col2"])
wine_data_df_pandas = wine_data_df.toPandas()
cluster_centers_pandas = pd.DataFrame(model.clusterCenters, columns=["col1", "col2"])
cluster_centers_pandas.head()
plt.scatter(wine_data_df_pandas["col1"], wine_data_df_pandas["col2"]);
plt.scatter(cluster_centers_pandas["col1"], cluster_centers_pandas["col2"], color="red", marker="x"
      
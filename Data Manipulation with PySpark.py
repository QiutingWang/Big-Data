###Data Manipulation with PySpark###

##Create Columns##use .withColumn()
#First, a string with the name of your new column, and second the new column itself.
df=df.withColumn("newCol", df.oldCol+1)
#normal usage:
df = df.withColumn("newCol", df.oldCol+Formula)

##Filtering data##.filter() is similar to where clause in SQL
flights.filter("air_time > 120").show() #we give a string in this clause
flights.filter(flights.air_time > 120).show() #it will return a column of boolean values

##Selecting##.select(), can be column name as a string, column object(df.colName)
#when it is column object: addition or subtraction on the column to change data contained it .withColumn()
#.select() returns only the columns you specify, while .withColumn() returns all the columns of the DataFrame in addition to the one you defined.
#normally we use .select()
# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest")
# Select the second set of columns(.withColumn method)
temp = flights.select(flights.origin, flights.dest, flights.carrier)
# Define first filter
filterA = flights.origin == "SEA"
# Define second filter
filterB = flights.dest == "PDX"
# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)

#Rename Column:
#.alias() 
flights.select((flights.air_time/60).alias("duration_hrs"))
#.selectExpr() as a string
flights.selectExpr("air_time/60 as duration_hrs")
# withColumnRenamed('orignial name','rename name'):
airports = airports.withColumnRenamed("faa", "dest")

##Aggregating##.min() .max() .count() .avg() .sum() .groupby()
df.groupBy().min("col").show()
#EG:
#Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()
+-------------+
|min(distance)|
+-------------+
|          106|
+-------------+
##Grouping and Aggregating##PySpark has a whole class devoted to grouped data frames: pyspark.sql.GroupedData
#EG:
# Group by tailnum
by_plane = flights.groupBy("tailnum")
# Number of flights each plane made
by_plane.count().show()
<script.py> output:
    +-------+-----+
    |tailnum|count|
    +-------+-----+
    | N442AS|   38|
    | N102UW|    2|
    | N36472|    4|
    | N38451|    4|
    | N73283|    4|
    | N513UA|    2|
    | N954WN|    5|
    | N388DA|    3|
    | N567AA|    1|
    | N516UA|    2|
    | N927DN|    1|
    | N8322X|    1|
    | N466SW|    1|
    |  N6700|    1|
    | N607AS|   45|
    | N622SW|    4|
    | N584AS|   31|
    | N914WN|    4|
    | N654AW|    2|
    | N336NW|    1|
    +-------+-----+
    only showing top 20 rows
# Group by origin
by_origin = flights.groupBy("origin")
# Average duration of flights from PDX and SEA
by_origin.avg("air_time").show()   
    +------+------------------+
    |origin|     avg(air_time)|
    +------+------------------+
    |   SEA| 160.4361496051259|
    |   PDX|137.11543248288737|
    +------+------------------+

#.agg() pass an aggregate column expression 
# Import pyspark.sql.functions as F,contains many useful functions for computing things like standard deviations.
import pyspark.sql.functions as F
# Group by month and dest
by_month_dest = flights.groupBy("month", "dest")
# Average departure delay by month and destination
by_month_dest.avg("dep_delay").show()
# Standard deviation of departure delay
by_month_dest.agg(F.stddev("dep_delay")).show()

<script.py> output:
    +-----+----+--------------------+
    |month|dest|      avg(dep_delay)|
    +-----+----+--------------------+
    |   11| TUS| -2.3333333333333335|
    |   11| ANC|   7.529411764705882|
    |    1| BUR|               -1.45|
    |    1| PDX| -5.6923076923076925|
    |    6| SBA|                -2.5|
    |    5| LAX|-0.15789473684210525|
    |   10| DTW|                 2.6|
    |    6| SIT|                -1.0|
    |   10| DFW|  18.176470588235293|
    |    3| FAI|                -2.2|
    |   10| SEA|                -0.8|
    |    2| TUS| -0.6666666666666666|
    |   12| OGG|  25.181818181818183|
    |    9| DFW|   4.066666666666666|
    |    5| EWR|               14.25|
    |    3| RDM|                -6.2|
    |    8| DCA|                 2.6|
    |    7| ATL|   4.675675675675675|
    |    4| JFK| 0.07142857142857142|
    |   10| SNA| -1.1333333333333333|
    +-----+----+--------------------+
    only showing top 20 rows
    
    +-----+----+----------------------+
    |month|dest|stddev_samp(dep_delay)|
    +-----+----+----------------------+
    |   11| TUS|    3.0550504633038935|
    |   11| ANC|    18.604716401245316|
    |    1| BUR|     15.22627576540667|
    |    1| PDX|     5.677214918493858|
    |    6| SBA|     2.380476142847617|
    |    5| LAX|     13.36268698685904|
    |   10| DTW|     5.639148871948674|
    |    6| SIT|                  null|
    |   10| DFW|     45.53019017606675|
    |    3| FAI|    3.1144823004794873|
    |   10| SEA|     18.70523227029577|
    |    2| TUS|    14.468356276140469|
    |   12| OGG|     82.64480404939947|
    |    9| DFW|    21.728629347782924|
    |    5| EWR|     42.41595968929191|
    |    3| RDM|      2.16794833886788|
    |    8| DCA|     9.946523680831074|
    |    7| ATL|    22.767001039582183|
    |    4| JFK|     8.156774303176903|
    |   10| SNA|    13.726234873756304|
    +-----+----+----------------------+
    only showing top 20 rows

##Join##
dataframe1.join(dataframe2,on 'the name of the key column(s) as a string', how='specifies the kind of join to perform')
# Join the DataFrames
flights_with_airports = flights.join(airports, on="dest", how="leftouter")



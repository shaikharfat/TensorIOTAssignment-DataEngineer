# TensorIOTAssignment-DataEngineer

Data file name: Electronics_5.json

Schema of file:
root
 |-- asin: string (nullable = true)
 |-- helpful: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- overall: double (nullable = true)
 |-- reviewText: string (nullable = true)
 |-- reviewTime: string (nullable = true)
 |-- reviewerID: string (nullable = true)
 |-- reviewerName: string (nullable = true)
 |-- summary: string (nullable = true)
 |-- unixReviewTime: long (nullable = true)

Summary of data (columns) file:
 asin : it a unique code for a product
 overall : it shows the rating for the product
 reviewText : it shows the text review left by the reviewer
 reviewTime : it is the date on which review was posted
 reviewerID : ID of the reviewer
 reviewerName : Name of the reviewer
 summary : summary of the review posted

Objectives:

1)Download a review file with a million reviews & read the million reviews
- File downloaded from http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Electronics_5.json.gz consisting of 1.6 million reviews
- Read it using spark.read.json since the file is in json format.
  
2)Item having the least rating
- To find the item having least rating column overall is used, and based on the lowest rating(i.e 1.0) the output is shown
+--------------------+------------+----------+
|     min(reviewerID)|min(overall)| min(asin)|
+--------------------+------------+----------+
|A000715434M800HLC...|         1.0|0528881469|
+--------------------+------------+----------+

3)Item having most rating
- To find the item having least rating column overall is used, and based on the max rating(i.e 5.0) the output is shown
- It is done in 2 ways one is by creating a tuple of overall and asin and applying max aggregation on them and then displaying.
+----------------+----------+
|      max_rating|      asin|
+----------------+----------+
|[5.0,B00LGQ6HL8]|B00LGQ6HL8|
+----------------+----------+
- In 2nd method aggregation method is directly applied.
+---------------+------------+----------+
|max(reviewerID)|max(overall)| max(asin)|
+---------------+------------+----------+
|  AZZYW4YOE1B6E|         5.0|B00LGQ6HL8|
+---------------+------------+----------+
- Output are similar in both cases 

4)Item having the longest review
- Using pyspark.sql.length library length of characters is extracted from column 'reviewText' and max aggregation is applied to it, giving us the row with longest character count (i.e longest review)

+-------------+----------+--------------------+---------+-------+
|   reviewerID|      asin|          reviewText|wordcount|overall|
+-------------+----------+--------------------+---------+-------+
|AZZYW4YOE1B6E|B00LGQ6HL8|~~~~~~~~~~~~~~~~~...|    32703|    5.0|
+-------------+----------+--------------------+---------+-------+

5)Transform: change the date MM-DD-YYYY format
- The date originally was in string format '06 2, 2013', it was converted to string '06-2-2013' using regex_replace and then converted to dateformat using to_date() function.
+----------+------------------+
|      date|date_in_dateFormat|
+----------+------------------+
| 06-2-2013|        2013-06-02|
|11-25-2010|        2010-11-25|
| 09-9-2010|        2010-09-09|
|11-24-2010|        2010-11-24|
|09-29-2011|        2011-09-29|
+----------+------------------+

6)Show a desired data frame operation which you learnt recently
- Recently been learning and working with UDFs and have written a UDF to grade the rating based on conditions, if rating more than 4 then it's 'good' otherwise 'bad'.
+-------+----------+-----+
|overall|      asin|Grade|
+-------+----------+-----+
|    5.0|0528881469| good|
|    1.0|0528881469|  bad|
|    3.0|0528881469|  bad|
|    2.0|0528881469|  bad|
|    1.0|0528881469|  bad|
+-------+----------+-----+
7)Convert the whole file into Parquet file after transforming.
- File converted to parquet on writing it as parquet into the local system using dataframe.read.parquet("\path")

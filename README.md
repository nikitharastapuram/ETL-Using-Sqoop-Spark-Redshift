# ETL-Using-Sqoop-Spark-Redshift

**PROBLEM STATEMENT**
Banks have to refill the ATMs when the money goes below a specific threshold limit. This depends on the activity and the area where a particular ATM is located as well as the weather, day of the week, etc.
Spar Nord Bank is trying to observe the withdrawal behaviour and the corresponding dependent factors to optimally manage the refill frequency. Apart from this, other insights also have to be drawn from the data.

**DATA SOURCE**
The dataset used is available at the location: https://www.kaggle.com/sparnord/danish-atm-transactions 

**PROJECT FLOW**

**1. EXTRACTION**
Imported data from Amazon RDS to HDFS using Sqoop and listed the imported data inside HDFS
Created input schema using StructType, read the data using the input schema created and verifying the data using count function

**2. TRANSFORM**
Created a dataframe for Location Dimension according to Target Dimension model. Cleaned and transformed the data for Location dimension
Created a dataframe for Card Type Dimension according to Target Dimension model. Cleaned and transformed the data for Card Type dimension
Created a dataframe for ATM Dimension according to Target Dimension model. Cleaned and transformed the data for ATM dimension
Created a dataframe for Date Dimension according to Target Dimension model. Cleaned and transformed the data for Date dimension

Transformed the data for this fact table

**3. LOADING**
Loaded the various tables to S3 bucket in csv format

**4. SET UP FOR ANALYSIS**
Created a RedShift cluster
Set up the Database in RedShift cluster and run the appropriate queries for each dimension and fact tables
Loaded the tables from S3 bucket to the RedShift Cluster

**5. ANALYSIS**
PERFORMED ANALYSIS ON THE FOLLOWING QUERIES 
1. Top 10 ATMs where most transactions are in the ’inactive’ state
2. Number of ATM failures corresponding to the different weather conditions recorded at the time of the transactions
3. Top 10 ATMs with the most number of transactions throughout the year
4. Number of overall ATM transactions going inactive per month for each month
5. Top 10 ATMs with the highest total amount withdrawn throughout the year
6. Number of failed ATM transactions across various card types
7. Top 10 records with the number of transactions ordered by the ATM_number, ATM_manufacturer, location, weekend_flag and then total_transaction_count, on weekdays and on weekends throughout the year
8. Most active day in each ATMs from location "Vejgaard"

# data-lake-spark-s3

### Introduction
A startup named Sparkify is a digital music streaming service that gives access to millions of songs and other content from artists all over the world. The company wants to analyze the users and songs data that it is currently collecting on their streaming app. The analytics team is interested in understanding what songs, genre or artists users are listening to, so that they can provide targeted marketing to increase the brand value and revenue of the company. The data currently resides in a Amazon S3 with a directory of JSON logs on user activity on the app, and a directoy of JSON metadata on the available songs on the app.
The company is expecting a Data Engineer to build an ETL pipeline to extract the data from S3 buckets, process them using Spark and load the data back into S3 into a data model consisting of fact and dimension tables. This would enable the analytics team to gather insights from the data to increase customer engagement on the app.

### Project Dataset
The project uses two datasets, one for the songs metadata and other one for the user activity on the app. The datasets are stored on AWS S3.
- Songs Data: s3://udacity-dend/song-data
- Logs Data: s3://udacity-dend/log-data

### Data Model
The data model resembles a star schema with one Fact table, songplays and four dimension table: users, songs, artists and time. A star schema provides the advantages of higher query performance, built-in referential integrity and ease of understanding.
![Data Model](https://github.com/abhi-verma/redshift_data_warehouse/blob/master/img/DataModelSparkify.PNG?raw=true)

### Project Structure
- dl.cfg: This is a configuration file which stores the AWS Access Key Id and Secret Access Key. The user would be used to access the S3 service.
- etl.py: This script contains the ETL pipelines. It first reads json data from the log-data and song-data S3 buckets, processes them using Spark, by filtering, modifying and adding derived columns and uploads the data back to abhi-spark-data-lake bucket into a data model consisting of fact and dimension tables.

### Project Steps
1. In a AWS account, create a user with access to S3 and copy the access key id and secret access key to the dl.cfg file.
2. Create a S3 bucket in the user account created in step 1 and replace the bucket name in output_data variable in etl.py file.
3. Run the etl.py file to load the data from S3, process them using Spark data frames and then insert data into the fact and dimension tables in the S3 bucket created in Step 2.
4. Verify the tables and the data on the S3 bucket.

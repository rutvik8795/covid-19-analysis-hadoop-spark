Given below are the commands used to run all the tasks:

1) For Hadoop task 1: 
hadoop jar Covid19.jar Covid19_1 /cse532/input/covid19_full_data.csv true /cse532/output/

2) For Hadoop task 2:
hadoop jar Covid19.jar Covid19_2 /cse532/input/covid19_full_data.csv 2020-02-01 2020-03-31 /cse532/output/

3) For Hadoop task 3:
hadoop jar Covid19.jar Covid19_3 /cse532/input/covid19_full_data.csv /cse532/cache/populations.csv /cse532/output/

For Spark task 1:
1) spark-submit --class SparkCovid19_1 SparkCovid19.jar hdfs://localhost:9000/cse532/input/covid19_full_data.csv 2020-02-01 2020-03-31 hdfs://localhost:9000/cse532/output/

For Spark task 2:
1) spark-submit --class SparkCovid19_2 SparkCovid19.jar hdfs://localhost:9000/cse532/input/covid19_full_data.csv hdfs://localhost:/9000/cse532/cache/populations.csv hdfs://localhost:9000/cse532/output/
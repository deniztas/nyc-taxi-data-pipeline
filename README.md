# nyc-taxi-data-pipeline
## About Project
Data pipeline of NYC taxis historic data

This repo provides scripts to download and preprocess data for billions of taxi rides that started in New York City for last 3 years. The raw data comes from [historical data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) of **The New York City Taxi and Limousine Commission**

### Built With
-  Python
-  Spark
-  Airflow

## Getting Started
### Prerequisites
-  python 3.7.x
-  Download Docker if does not exist for see the pipeline
### Instructions
1. **Execute ```docker build -t myimage .``` command in the directory where the docker file is located**
2. **Execute ```docker run -d --rm -v output_parquet:/output_parquet -v output_avro:/output_avro -p 8080:8080 myimage``` command**

+ This command build and docker image that containd Airflow. You will access the Airflow user interface from ```http://localhost:8080/``` after all requirements are installed. This is what the pipeline looks like.
3. **Before the execute following command you may want to minimize the data to be downloaded.**

+ Go to `data_pipeline/config.yaml`. You will see a parameter named `retention_N_months`. This parameter keeps how many months to go back from 2020-12. If you don't want to install whole 3 years data, you can reduce this value.
4. **Commands**
- Open  a terminal in the project folder.
- Execute following command to set python path the project path. ```export PYTHONPATH="${PYTHONPATH}:/path/to/your/project/"``` for For UNIX (Linux, OSX, ...), ```set PYTHONPATH=%PYTHONPATH%;C:\path\to\your\project\``` for Windows
- Execute ```pip insyall requirements.txt``` command.
- ```spark-submit --jars spark/jars/spark-avro_2.11-2.4.3.jar data_pipeline/etl_green_taxi.py```
- After this command parquet and avro files will be in the output_parquet and output_avro folders for green taxi
- ```spark-submit --jars spark/jars/spark-avro_2.11-2.4.3.jar data_pipeline/etl_yellow_taxi.py```
- After this command parquet and avro files will be in the output_parquet and output_avro folders for yellow taxi
4. **~~Click the `nyc_taxi_etl` and Trigger DAG~~**
- Output folders in the container are not synchronous with the local output folders.
### Pipeline Definition
-   `green_taxi` and `yellow_taxi` tasks execute the `data_pipeline/green_taxi_etl.py` and `data_pipeline/yellow_taxi_etl.py`. These classes derived from `etl_base.py`
-   csv url, output parquet and input parquet paths are retrieved with the help of config.py. `run_etl` function in the base is executed from sub classes. And pipeline starts.
-   `run_extract` function calculates dynamically the path of the csv file as much as value of the `data_retention` parameter and downloads it if it is not in the relevant directory.
-   run_transform_and_load function reads csv files and writes file as parquet and avro format. This is done for the number of files the `raw_file_names_and_year` variable holds. `raw_file_names_and_year` is a tuple that keeps file name and year. This year information is used on data cleaning.
-   Read and write functions are in `helper/read_write.py`. 
-   After csv is read, transformers function runs sequentially. transformers defined sub classes is a list contains data transformation functions.
-   `rename_columns` function rename datetime columns to same for yellow and green taxi data
-   `remove_redundant_year_records` function filters data to keep records on relevant year.
-   seperate_date_columns seperate date time columns to year, month, hour, day of week. Files will partition according to these columns while they are written.
-   `convert_to_boolean` function convert type of `store_and_fwd_flag` column to boolean.
-   After there transform functions are executed dataframe is written as parquet and avro format.
-   3 queries run on `data_pipeline/correctness_queries.py` after all files are written. There are unit tests for this functions in `tets/tes_correctness_queries.py` path. You can see the output format from unit tests.

-   This pipeline runs on Airflow. The dag is in `/dag/etl_pipeline.py` path.

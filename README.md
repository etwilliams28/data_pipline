# data_pipline
Using apache airflow to pipline data from S3 to Redshift

## Overview

A music streaming service is looking to implement automation and quality checks to their data collection process.\
An ideal solution is to use Apache Airflow to take care of the piplining process. 

## Structure

Airlflow 
* dags
  * dac_example_dag.py
* plugins
  * helpers
    * init__.py
    * sql_queries.py
                 
* operators
  * init__.py
  * data_quality.py
  * load_dimension.py
  * load_fact.py
  * load_fact_practice.py
  *  stage_redshift.py  
  
*  __init__.py         

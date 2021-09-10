## What is Apache Airflow
Apache Airflow is a workflow engine that will easily schedule and run your complex data pipelines. It will make sure that each task of your data pipeline will get executed in the correct order and each task gets the required resources.

It will provide you an amazing user interface to monitor and fix any issues that may arise


## Installation steps
Let’s start with the installation of the Apache Airflow. Now, if already have pip installed in your system, you can skip the first command. To install pip run the following command in the terminal.
```bash
sudo yum install python2-pip
```
Next airflow needs a home on your local system. By default ~/airflow is the default location but you can change it as per your requirement.
```bash
export AIRFLOW_HOME=~/airflow
```
Airflow requires a database backend to run your workflows and to maintain them. Now, to initialize the database run the following command.
```bash
airflow db init
```
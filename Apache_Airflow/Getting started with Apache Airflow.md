## What is Apache Airflow
Apache Airflow is a workflow engine that will easily schedule and run your complex data pipelines. It will make sure that each task of your data pipeline will get executed in the correct order and each task gets the required resources.

It will provide you an amazing user interface to monitor and fix any issues that may arise


## Installation steps
Let’s start with the installation of the Apache Airflow. Now, if already have pip installed in your system, you can skip the first command. To install pip run the following command in the terminal.
```bash
sudo yum install python3-pip
```
Next airflow needs a home on your local system. By default ~/airflow is the default location but you can change it as per your requirement.
```bash
export AIRFLOW_HOME=~/airflow
```
Now, install the apache airflow using the pip with the following command.
```bash
pip3 install apache-airflow
```

Airflow requires a database backend to run your workflows and to maintain them. Now, to initialize the database run the following command.
```bash
airflow db init
```
We have already discussed that airflow has an amazing user interface. To start the webserver run the following command in the terminal. The default port is 8080 and if you are using that port for something else then you can change it.

```bash
airflow webserver -p 8080
```
Now, start the airflow schedular using the following command in a different terminal. It will run all the time and monitor all your workflows and triggers them as you have assigned.
```bash
airflow scheduler
```
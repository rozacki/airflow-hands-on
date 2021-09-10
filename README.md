

## Installation Instructions


1) Install an IDE - we will use PyCharm Community Edition - https://www.jetbrains.com/pycharm/

1) Download and install miniconda for dependency and package management.

```sh
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh -O ~/miniconda.sh
bash ~/miniconda.sh -b -p $HOME/miniconda
```

The installer prompts “Do you wish the installer to initialize Miniconda3 by running conda init?” We recommend “yes”.

Note -  If you enter “no”, then conda will not modify your shell scripts at all. In order to initialize after the installation process is done, first run source <path to conda>/bin/activate and then run conda init.


2) Test you are able to use conda commands. 

```shell
conda env list 
```
This command will show you the available environments. If this is the 
first time you are installing conda, there will be just one, the base
environment.


3) Create a new environment with python 3.8 installed.

```shell
conda create -n airflow_hands_on python==3.8
```

Type Y when asked whether you want to proceed with creating the environment.

4) Activate the environment, using ```conda activate airflow_hands_on```. Check your python 
version - ```python --version```. It should match 3.8.x.
   

5) Install Airflow and requests package via pip into the conda environment

$ pip install "apache-airflow
$ pip install requests
$ pip install pandas

Then run below
```
# initialize the database
airflow db init

airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org

# start the web server, default port is 8080
airflow webserver --port 8080 -D

# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
airflow scheduler -D

# visit localhost:8080 in the browser and use the admin account you just
# created to login. Enable the example_bash_operator dag in the home page
```

After that step there should be ~/your-user-name/airflow/dags

Go to that folder and

$git clone git@github.com:rozacki/airflow-hands-on.git

This is where we will develop our data pipelines.

Open this location with your favourite IDE.   
   

### Appendix:

If wget is missing, brew may take opportunity to update some packages

$ brew install wget

If conda is not on your path

$ export PATH="$HOME/miniconda/bin:$PATH"

Initialize conda with shell of your choice (shell restart will be required)

$ conda init bash


### Parallel execution
Question: how to control parallelisation?

1. Change executor to LocalExecutor. It does not require setup for queue (redis/rabbitmq)
2. Install and provision Postgress database by calling 

airflow db init

And changing sql_alchemy_conn

sql_alchemy_conn = postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db

Check http://airflow.apache.org/docs/apache-airflow/stable/faq.html#how-to-improve-dag-performance
But default setting allow you to run 16 the same dags concurrently

```` 
# This defines the maximum number of task instances that can run concurrently in Airflow
# regardless of scheduler count and worker count. Generally, this value is reflective of
# the number of task instances with the running state in the metadata database.
parallelism = 0
````

````
# The maximum number of task instances allowed to run concurrently in each DAG. To calculate
# the number of tasks that is running concurrently for a DAG, add up the number of running
# tasks for all DAG runs of the DAG. This is configurable at the DAG level with ``concurrency``,
# which is defaulted as ``dag_concurrency``.
dag_concurrency = 32
````
````
# The maximum number of active DAG runs per DAG. The scheduler will not create more DAG runs
# if it reaches the limit. This is configurable at the DAG level with ``max_active_runs``,
# which is defaulted as ``max_active_runs_per_dag``.
max_active_runs_per_dag = 16
````

Default values were used for DAG and task specific settings

I used CLI to trigger dags

````
airflow dags trigger event_driven
````

I set up start_date to the past 2021-09-24. When airflow finished processing passed dates I had a nice set
of DAG runs to retry and run un parallel including 08-24
I used:

````
airflow tasks clear -s 2021-08-24 event_driven
````

To retry failed dag runs I used:
````
airflow tasks clear --only-failed -s 2021-08-24 event_driven
````

### Passing parameters
Questions:

how to pass arguments during manual trigger i.e. CLI, UI, API?

can we pass parameters using xcom?
yes
can we still retry and oryginal arguments both from command line and passed via
xcom will be preserved?
Yes

````
airflow dags trigger -c '{"snaphot_id";"1"}' event_driven
````
Load test using API is available here load_test.py.
I 
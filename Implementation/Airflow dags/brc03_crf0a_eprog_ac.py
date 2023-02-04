import os
import airflow
import pendulum
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------
#                          DAG credentials
# ---------------------------------------------------------------
# The user who runs the tasks must be a technical account NOT a personal account
# and has access to the corresponding BRC
V_USER = 'sd01865'

# DO NOT MODIFY THIS SECTION
# Create DAG_ID corresponding to the filename without the extension
directory_path_len = len(os.path.dirname(__file__))
if directory_path_len > 0:
    directory_path_len = directory_path_len + 1
V_DAG_ID = os.path.splitext(__file__)[0][directory_path_len:]

# ---------------------------------------------------------------
#                         Default arguments
# ---------------------------------------------------------------
# These args will be passed to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    # DO NOT MODIFY THE OWNER
    'owner': V_USER,
    # People who receive emails
    'email': ['salma.ouardi@external.stellantis.com'],
    # Email when task fails
    'email_on_failure': True,
    # Email when task retries
    'email_on_retry': False,
    # Number of retries when a task failed
    'retries': 0,
    # Delay between two retries
    'retry_delay': timedelta(minutes=5),
    # The task will be executed ONLY if this task ran successfully in previous launch
    'depends_on_past': False,
    # catchup option is used to catch up all missed runs between the start date and the date when the DAG is installed
    'catchup_by_default': False
    # The date when the DAG must stop (no more launch)
    # 'end_date': datetime(2016, 1, 1),
    # Max duration before the task is marked as failed
    # 'execution_timeout': timedelta(seconds=300),
    # Callback functions for success, failure and retry status
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # Trigger to launch task depending on the result of the previous tasks
    # see more at : https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#concepts-trigger-rules
    # 'trigger_rule': 'all_success'
}

# ---------------------------------------------------------------
#                           Define a DAG
# ---------------------------------------------------------------
# The first date at which the DAG must be launched
# Tip: if you want to run your job for the first time on 2021/07/02, set START_DATE to 2021/07/01
# The default Airflow timezone is UTC, it is then necessary to specify the timezone
# in the START_DATE before setting the schedule interval
local_tz = pendulum.timezone('Europe/Paris')
START_DATE = datetime(2022, 7, 11, tzinfo=local_tz)

# Set only description, schedule_interval, start_date
# Example: the DAG is executed every day at 8am
dag = DAG(
    V_DAG_ID,
    default_args=default_args,
    description='eProg AC Data Pipeline (Airflow test)',
    schedule_interval='0 1 * * *',
    start_date=START_DATE,
    catchup=False
)

# ---------------------------------------------------------------
#                    Define and set DAG's tasks
# ---------------------------------------------------------------
# To use variables in task command, the command must be defined outside of the operator
UNXAPPLI = '/gpfs/user/sd01865/crf0a'
CMD_EPROG_AC = 'cd ' + UNXAPPLI + ';script/eprog_ac_data_pipeline.sh;'

with dag:
    # Only BashOperator can be used
    t1 = BashOperator(
        # Task id must be unique in the DAG
        task_id='eprog_ac',
        # To use variables in command, the command must be defined outside of the operator
        bash_command=str(CMD_EPROG_AC),
    )

    
    # Organize dependencies between tasks
    t1

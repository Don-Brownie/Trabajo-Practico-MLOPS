chmod +x install_airflow.sh

./install_airflow.sh

source airflow_env/bin/activate


export AIRFLOW_HOME=~/airflow


airflow db init

airflow_admin_user.sh

airflow webserver --port 8080 & airflow scheduler &

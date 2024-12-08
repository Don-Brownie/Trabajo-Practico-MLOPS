# Define the path to your airflow.cfg file (you may need to adjust this path)
AIRFLOW_CFG_PATH="$HOME/airflow/airflow.cfg"

# Update 'load_examples' to False
sed -i 's/^load_examples = True/load_examples = False/' "$AIRFLOW_CFG_PATH"

# Update the Airflow configuration file to use the LocalExecutor and set parallelism to 2
sed -i 's/^executor = SequentialExecutor/executor = LocalExecutor/' "$AIRFLOW_CFG_PATH"
sed -i 's/^parallelism = 32/parallelism = 2/' "$AIRFLOW_CFG_PATH"

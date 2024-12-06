# Define the path to your airflow.cfg file (you may need to adjust this path)
AIRFLOW_CFG_PATH="$HOME/airflow/airflow.cfg"

# Update 'load_examples' to False
sed -i 's/^load_examples = True/load_examples = False/' "$AIRFLOW_CFG_PATH"


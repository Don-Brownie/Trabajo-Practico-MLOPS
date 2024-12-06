sed -i 's/^load_examples = True/load_examples = False/' "$AIRFLOW_CFG_PATH"

# Define the path to your airflow.cfg file (you may need to adjust this path)
AIRFLOW_CFG_PATH="$HOME/airflow/airflow.cfg"

# Define the new path for the dags_folder
NEW_DAGS_FOLDER="/home/ubuntu/Trabajo-Practico-MLOPS/'Hosting Airlfow'/DAGs"

# Use sed to replace the dags_folder path
sed -i "s|^dags_folder = .*|dags_folder = $NEW_DAGS_FOLDER|" "$AIRFLOW_CFG_PATH"

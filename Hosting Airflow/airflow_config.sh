# Define the path to your airflow.cfg file
AIRFLOW_CFG_PATH="$HOME/airflow/airflow.cfg"

# Cambia la conexión a la base de datos de SQLite (por defecto) a PostgreSQL en RDS
sed -i 's|sql_alchemy_conn = postgresql+psycopg2://grupo-17-rds:yourpassword123@grupo-17-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com|' "$AIRFLOW_CFG_PATH"

# Cambia el executor a LocalExecutor
sed -i 's/^executor = SequentialExecutor/executor = LocalExecutor/' "$AIRFLOW_CFG_PATH"

# Configura el parallelism a 2 para no saturar el servidor
sed -i 's/^parallelism = 32/parallelism = 2/' "$AIRFLOW_CFG_PATH"

# Desactiva la carga de ejemplos si no es necesario
sed -i 's/^load_examples = True/load_examples = False/' "$AIRFLOW_CFG_PATH"

# (Opcional) Configura los workers del webserver a 1 para reducir el consumo de recursos
sed -i 's/^workers = 4/workers = 1/' "$AIRFLOW_CFG_PATH"

# Disables variable interpolation in the airflow.cfg file
sed -i '/\[core\]/,/^\[/{/interpolate/ s/.*/interpolate = false/}' "$AIRFLOW_CFG_PATH"

echo "Airflow configuration updated successfully!"

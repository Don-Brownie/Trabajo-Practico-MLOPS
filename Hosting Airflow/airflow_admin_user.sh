#Está estilizado para facilidad de lectura, pero este código es el que realmente cambia el nombre del usuario maestro de airflow

airflow users create \
    --username admin \         # Username for the admin account
    --firstname Ad \           # First name of the admin user
    --lastname Min \           # Last name of the admin user
    --role Admin \             # Role assigned to the user (Admin)
    --email ad@min.com         # Email address of the admin user

#!/bin/bash

# Step 1: Update and install dependencies
sudo apt update -y
sudo apt install -y software-properties-common build-essential zlib1g-dev \
libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev \
libffi-dev wget curl libbz2-dev liblzma-dev libsqlite3-dev python3.10 \
python3.10-venv python3.10-dev python3.10-distutils

# Step 2: Remove any existing system `pip`
sudo apt remove -y python3-pip || true

# Step 3: Install `pip` for Python 3.10 using get-pip.py
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10

# Step 4: Verify pip installation
pip --version

# Step 5: Set Airflow version and Python version
AIRFLOW_VERSION=2.8.4
PYTHON_VERSION=3.10
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Step 6: Create a virtual environment
python3.10 -m venv airflow_env
source airflow_env/bin/activate

# Step 7: Upgrade pip inside the virtual environment
pip install --upgrade pip

# Step 8: Install Apache Airflow using constraints
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Step 9: Verify Airflow installation
airflow version

# Step 10: Deactivate virtual environment
deactivate

echo "Apache Airflow has been successfully installed!"

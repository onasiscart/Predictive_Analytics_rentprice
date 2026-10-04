#!/bin/bash

# Project path configuration
PROJECT_ROOT=$(pwd)
export AIRFLOW_HOME="$PROJECT_ROOT/airflow_home"
CONTAINER_NAME="mongodb_project"
MONGO_DATA_DIR="$PROJECT_ROOT/data/mongo_db_data"

# 1. Docker daemon check
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker daemon is not running. Please start Docker Desktop or the docker service."
    exit 1
fi

# 2. Cleanup previous environment
echo "Cleaning up previous deployment..."
# Stop and remove existing container to release ports
docker rm -f $CONTAINER_NAME > /dev/null 2>&1

# Remove existing database files (requires sudo due to Docker root permissions)
if [ -d "$MONGO_DATA_DIR" ]; then
    echo "Removing old database volume..."
    sudo rm -rf "$MONGO_DATA_DIR"
fi
mkdir -p "$MONGO_DATA_DIR"

# 3. Initialize MongoDB container
echo "Starting MongoDB container..."
docker run --name $CONTAINER_NAME \
    -p 27017:27017 \
    -v "$MONGO_DATA_DIR":/data/db \
    -d mongo:6.0

# 4. MongoDB Health Check loop
# Ensure the database is ready before initializing dependent services
echo "Waiting for MongoDB to accept connections..."
MAX_ATTEMPTS=15
ATTEMPT=0
while ! docker exec $CONTAINER_NAME mongosh --eval "db.adminCommand('ping')" --quiet > /dev/null 2>&1; do
    if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
        echo "Error: MongoDB failed to start within the expected time."
        exit 1
    fi
    ATTEMPT=$((ATTEMPT+1))
    echo "Retry $ATTEMPT/$MAX_ATTEMPTS: Database is still initializing..."
    sleep 2
done
echo "MongoDB is ready."

# 5. Python Virtual Environment setup
echo "Setting up Python virtual environment..."
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    echo "Warning: requirements.txt not found. Installing base dependencies..."
    pip install pyspark pymongo apache-airflow
fi

# 6. Airflow initialization
echo "Initializing Airflow database and admin user..."
airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Sync DAGs directory
mkdir -p "$AIRFLOW_HOME/dags"
ln -sf "$PROJECT_ROOT/dags/"* "$AIRFLOW_HOME/dags/"

# Force DAG parsing to avoid delay in the UI
airflow dags reserialize

# 7. Start Airflow background services
echo "Starting Airflow Webserver and Scheduler..."

# Definim el port en una variable per si el profe el vol canviar fàcilment
AIRFLOW_PORT=8085 

# Matem qualsevol procés que estigui usant aquest port abans de començar
fuser -k ${AIRFLOW_PORT}/tcp > /dev/null 2>&1

nohup airflow webserver --port $AIRFLOW_PORT > webserver.log 2>&1 &
nohup airflow scheduler > scheduler.log 2>&1 &

echo "---------------------------------------------------------"
echo "Setup process finished successfully."
echo "Airflow UI: http://localhost:$AIRFLOW_PORT"
echo "MongoDB Port: 27017"
echo "---------------------------------------------------------"
echo "Setup process finished successfully."
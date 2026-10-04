#!/bin/bash

# ---- SHUT DOWN SERVICES ----

# STOP AIRFLOW PROCESSES
echo "Stopping Airflow services..."
pkill -9 -f "airflow webserver" # to ensure all child processes are terminated
pkill -9 -f "airflow scheduler"
echo "Airflow processes terminated."

#STOP MONGODB CONTAINER
CONTAINER_NAME="mongodb_project"
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "Stopping MongoDB Docker container: $CONTAINER_NAME..."
    docker stop $CONTAINER_NAME
    # do not 'rm' the container to preserve its state and speed up 
    # the next startup.
else
    echo "MongoDB container is not running or was not found."
fi

# REMIND USER TO DEACTIVATE VIRTUAL ENVIRONMENT
echo "Environment shutdown process completed."
echo "Note: If you are still in the terminal session, please type 'deactivate' manually."

echo "---------------------------------------------------------"
echo "All services have been stopped successfully."
echo "---------------------------------------------------------"
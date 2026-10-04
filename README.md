# Barcelona Rental Prediction Pipeline (BDA Project)
---

This project implements an end-to-end Data Engineering pipeline using **Apache Airflow**, **Apache Spark**, and **MongoDB**. It automates the collection, formatting, and processing of Barcelona's rental and social data to perform predictive analysis on neighborhood prices.

Urban rental markets are driven by factors beyond mere real estate supply. In rapidly evolving metros like Barcelona, hyper-local livability drivers—specifically **urban safety (traffic hazards and incident density)** and **cultural accessibility (proximity to landmarks and institutions)** exert significant pressure on residential market valuations.

This project delivers an automated, scalable **Data Engineering & Predictive Analytics Platform** designed to ingest heterogeneous urban datasets, harmonize spatial-temporal granularities, and deploy machine learning models that classify neighborhood rental price tiers. 

### Key Technical Highlights
* **Automated Multi-Source Ingestion:** Batch and streaming ingestion using **PySpark** handling multi-cadence datasets (Daily pricing, Weekly cultural sites, Annual accident records).
* **Lakehouse Architecture:** Structured multi-layer data pipeline progressing from a Raw Landing Zone to a **MongoDB** Formatted Zone (Idempotent upserts, strict typing) and a **Delta Lake** Exploitation Zone (ACID transactions, time travel).
* **Robust Granularity Harmonization:** Automated spatial-temporal roll-ups aggregating micro-level data into normalized `(Neighborhood, Year)` keys with semantic null-handling logic.
* **Production MLOps Lifecycle:** Distributed training with **Spark ML**, hyperparameter cross-validation, experiment tracking, and automated model registration using **MLflow**.
* **One-Command Orchestration:** Fully containerized setup via **Docker**, orchestrated using **Apache Airflow** with real-time terminal dashboards and automated failure diagnostics.

### Technical & Architecture Report

For a deep dive into architectural trade-offs, schema evolution strategies, target-driven join semantics, and model benchmarking results, refer to the report containing all design decisions and results:
 **[Read the Design Decisions & Engineering Report (PDF)](design_decisions_report.pdf)**

---
## 1. Prerequisites
Ensure the following are installed in your Linux environment:
* **Python 3.10+**
* **Java 8 or 11** (Required for Apache Spark)
* **Docker** (Used by the script to automatically launch a MongoDB container)

## 2. Environment Setup
We recommend using a virtual environment to manage dependencies:
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```
## 3. Starting Services
The following script automates the setup. It creates the virtual environment, installs Airflow and dependencies, launches a **MongoDB container** (Docker), and initializes the Airflow database and admin user.
```
chmod +x start_env.sh
./start_env.sh
```
## 4. Running the Pipeline

You can trigger and monitor the DAG in two ways:

**Option A: Terminal**
Run our custom orchestrator. It will trigger a new DAG run and provide a real-time dashboard with automated error diagnostics:
```
#@main_project_folder>  python3 run.py
```

**Option B: Airflow Web Interface (GUI)**
* Visit http://localhost:8080
* Credentials:
    * Username: admin
    * Password: admin
* Find bcn_rental_prediction_pipeline in the DAG list.
* Unpause the DAG (toggle the switch to blue).
* Click on the DAG name and use the "Play" button (Trigger DAG) to start.

*Note: If you run the python script, you can also open the browser to see how the run advances using the same URL.* 

## 5. Shutting Down

To safely terminate all background processes, including the Airflow scheduler, the webserver, and the MongoDB Docker container, run:
```
chmod +x stop_env.sh
./stop_env.sh
```
*Note:Remember to type `deactivate` in your terminal to exit the Python virtual environment. *

## 6. Project Structure
This is a brief guide for orientation purposes and in case anything should fail: 

```
proj2_BDA/
├── dags/                    # DAG orchestration
│   └── orchestration.py     # orchestrator (airflow)
├── pipelines/               # Data engineering an Data analysis backbones
|   ├── utils.py
│   ├── collectors/         
│   ├── formatters/          
│   ├── transformers/        
│   └── models/              # ML training and evaluation
├── scripts/                 # Utility scripts
├── data/                    
│   ├── raw_data/            # Source files (price data read rom here)
│   ├── landing_zone/        
│   ├── formatted_zone/      # empty folder (data in MongoDB)
│   └── exploitation_zone/   
├── logs/                    # pipeline execution logs
├── mlresults/               # Data Analysis results csv and visualization notebook
├── mlruns/                  # M;LFlow run logs
├── config.yaml              # Global configuration
├── run.py                   # application trigger script and monitoring
├── requirements.txt         # dependencies 
├── README.md                # this file
├── start_env.sh             # Service initialization script
└── stop_env.sh              # Service termination script
```
---

Authors: 

Ona Siscart Noguer

Aina Vila Arbusà

Welcome to FireAnts dbt project!

### Step 0
- Python 3.10
- Gcloud

### Using the starter project

Try running the following commands:
- python3 -m venv venv
- source venv/bin/activate
- pip3 install -r requirements.txt
- gcloud auth login --update-adc
- cd fum_flows

If you want to check all SQL file, run:
- sh dbt_run_local.sh

If you want to check one SQL file, run:
- sh dbt_run_local.sh {SQL_file_name}.sql

SCD2 Partner Data Update Script
Overview
This Python script implements Slowly Changing Dimensions Type 2 (SCD2) logic to manage historical data in a BigQuery table. It takes an input table (Table1_Partners) containing partner data, checks for changes in key fields, and updates the output table (Table2_Partners_SCD2) with the appropriate historical records while preserving past data.

The script uses dynamic configuration through JSON or text files to specify the required parameters, making it flexible and easy to adapt to various tables and projects.

Features
Handles Slowly Changing Dimensions Type 2 (SCD2) for tracking changes in partner records.
Integrates with Google BigQuery to read and update tables.
Dynamic configuration through JSON or text files for flexibility.
Tracks changes in key fields (e.g., Name, Canton) and maintains historical records with Date_From, Date_To, and Is_valid fields.
Logs all activities to scd2_script.log for easy troubleshooting.

Requirements
Python 3.7+
Google Cloud SDK with authentication setup
Required Python libraries:
google-cloud-bigquery
pandas
pandas-gbq
uuid
Installation
To install the required dependencies, run:

pip install google-cloud-bigquery pandas pandas-gbq
Authentication
Ensure you have authenticated your Google Cloud SDK or are using a service account key file:

gcloud auth application-default login
Alternatively, set up the service account credentials:


export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-file.json"
Configuration
You can configure the script using either a JSON or text file to provide input parameters dynamically.

Example JSON Configuration (config.json)
json

{
    "project_id": "your_project_id",
    "dataset_id": "your_dataset_id",
    "input_table": "Table_1_Partners_Input",
    "output_table": "Table2_Partners_SCD2",
    "id_column": "PartnerID"
}

Usage
To run the script, use the following command in the terminal:

Using JSON Configuration:
python partner_history_tracker.py config.json json
or python partner_history_tracker.py make sure the config file is same location as the script

Parameters:
config.json The configuration file containing input parameters.
All script activities, including errors and updates, are logged in scd2_script.log:

INFO: Tracks major steps such as loading data, applying SCD2 logic, and writing results.
ERROR: Captures any issues that occur during the process.
Troubleshooting
Ensure that all required Python packages are installed.
Make sure that your Google Cloud project is correctly authenticated and that the credentials file is properly set up.
Check the partner_history_tracker.log file for detailed logs if any issues arise.

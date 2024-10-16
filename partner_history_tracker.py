import logging
import pandas as pd
import pandas_gbq 
import uuid
import json
from datetime import datetime
from google.cloud import bigquery

# Set up logging configuration
logging.basicConfig(
    filename='partner_history_tracker.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_parameters_from_json(json_file):
    with open(json_file, 'r') as file:
        return json.load(file)

def generate_technical_key():
    """Generate a unique technical key for each record."""
    return str(uuid.uuid4())

def apply_scd2_logic(client, dataset_id, input_table, output_table, id_column):
    """
    Apply Slowly Changing Dimensions Type 2 (SCD2) logic.
    """
    try:
        # Step 1: Load current data from input_table
        logging.info(f"Loading data from {input_table}.")
        query = f"SELECT * FROM `{dataset_id}.{input_table}`"
        df_new_data = client.query(query).to_dataframe()
        logging.info(f"Loaded {len(df_new_data)} records from {input_table}.")

        # Step 2: Load existing data from output_table (historical data)
        try:
            query = f"SELECT * FROM `{dataset_id}.{output_table}`"
            df_scd2 = client.query(query).to_dataframe()
            logging.info(f"Loaded {len(df_scd2)} records from {output_table}.")
        except Exception as e:
            logging.warning(f"Output table {output_table} does not exist. Creating a new empty DataFrame.")
            df_scd2 = pd.DataFrame(columns=df_new_data.columns.tolist() + ['TechnicalKey', 'Date_From', 'Date_To', 'Is_valid'])

        # Step 3: Ensure data types are consistent
        df_new_data[id_column] = df_new_data[id_column].astype(str)  # Convert to string for consistency
        df_scd2[id_column] = df_scd2[id_column].astype(str)  # Convert to string for consistency

        # Step 4: Process each record in the new data
        for _, new_row in df_new_data.iterrows():
            partner_id = new_row[id_column]
            logging.debug(f"Processing PartnerID: {partner_id}")

            existing_records = df_scd2[(df_scd2[id_column] == partner_id) & (df_scd2['Is_valid'] == 'Yes')]

            if not existing_records.empty:
                # Check for changes
                if not new_row.equals(existing_records.iloc[0][df_new_data.columns]):
                    logging.info(f"Detected changes for PartnerID: {partner_id}.")
                    # Invalidate the old record
                    df_scd2.loc[existing_records.index, 'Date_To'] = datetime.now().strftime('%Y-%m-%d')
                    df_scd2.loc[existing_records.index, 'Is_valid'] = 'No'
                    
                    # Insert the new record
                    new_record = new_row.copy()
                    new_record['TechnicalKey'] = generate_technical_key()
                    new_record['Date_From'] = datetime.now().strftime('%Y-%m-%d')
                    new_record['Date_To'] = '9999-12-31'
                    new_record['Is_valid'] = 'Yes'
                    
                    df_scd2 = pd.concat([df_scd2, pd.DataFrame([new_record])], ignore_index=True)
                    logging.info(f"Inserted updated record for PartnerID: {partner_id}.")
            else:
                # Insert the new record if it doesn't exist
                logging.info(f"Inserting new record for PartnerID: {partner_id}.")
                new_record = new_row.copy()
                new_record['TechnicalKey'] = generate_technical_key()
                new_record['Date_From'] = datetime.now().strftime('%Y-%m-%d')
                new_record['Date_To'] = '9999-12-31'
                new_record['Is_valid'] = 'Yes'
                
                df_scd2 = pd.concat([df_scd2, pd.DataFrame([new_record])], ignore_index=True)

        # Step 5: Write the updated DataFrame back to BigQuery
        pandas_gbq.to_gbq(df_scd2, f'{dataset_id}.{output_table}', project_id=client.project, if_exists='replace')
        logging.info(f"Table {output_table} updated with SCD2 logic and written back to BigQuery.")

    except Exception as e:
        logging.error(f"An error occurred during the SCD2 process: {str(e)}", exc_info=True)

def main(config_file, config_type='json'):
    try:
        # Load the configuration from the specified file type
        if config_type == 'json':
            config = load_parameters_from_json(config_file)
        else:
            raise ValueError(f"Unsupported config type: {config_type}")

        project_id = config['project_id']
        dataset_id = config['dataset_id']
        input_table = config['input_table']
        output_table = config['output_table']
        id_column = config['id_column']

        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)

        # Apply SCD2 logic
        apply_scd2_logic(client, dataset_id, input_table, output_table, id_column)

    except Exception as e:
        logging.error(f"Error in main function: {e}")

if __name__ == '__main__':
    # Example of how to run the script with a config file
    config_file = 'config.json'  # Or change to 'config.txt' for text file
    config_type = 'json'  # Use 'text' if using text config file
    main(config_file, config_type)

import pandas as pd
import json
import os
import duckdb
from tqdm import tqdm
import datetime
from constants import key, db, source_data



# Function to convert a month number to its abbreviation
def month_to_abbrev(month):
    month_abbreviations = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }
    return month_abbreviations.get(month, "InvalidMonth")


def create_parquet(json_file_path):
    # Read the year and month from the JSON file
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    year = data['year']
    month = data['month']

    # Convert the month number to its abbreviation
    month_abbrev = month_to_abbrev(month)

    # Construct the filename based on year and month
    filename = f"{month_abbrev}_{year}.parquet"

    # Create an empty DataFrame and save it as a Parquet file in the current directory
    empty_df = pd.DataFrame()
    empty_df.to_parquet(filename, index=False)

    # Return the filename for use later - in step 3.
    return filename


# Process permit data into parquet
def process_json(source_folder, destination_file):
    # Create a list to store the data
    data = []

    # Loop through the files in the source folder using tqdm for progress tracking
    # The process should take around 1 hour depending on the speed of your system
    for filename in tqdm(os.listdir(source_folder), desc="Processing files"):
        if filename.endswith('.json'):
            with open(os.path.join(source_folder, filename)) as json_file:
                json_data = json.load(json_file)
                if 'object_data' in json_data:
                    object_data = json_data['object_data']
                    # Loop through and extract the keys/values in the object_data dictionary
                    for key in object_data:
                        # Add the relevant data to the list
                        data.append({
                            'filename': filename,
                            'key': key,
                            'value': object_data[key]
                        })

    # Create a DataFrame with the completed data list
    df = pd.DataFrame(data)
    print("Dataframe Done")

    # Pivot the DataFrame to make the keys into columns
    df = df.pivot(index='filename', columns='key', values='value').reset_index()
    print("Pivot Done")

    # Write the DataFrame to a Parquet file
    df.to_parquet(destination_file, index=False)
    print(f"Wrote {df.shape[0]} rows to {destination_file}")


def update_date(file_path, new_file_path):
    # Read the Parquet file
    df = pd.read_parquet(file_path)

    # Extract the filename from the file path
    filename = os.path.basename(file_path)

    # Extract the year and month from the filename and convert them to integer
    month, year = filename.split('_')
    year = int(year.split('.')[0])
    months = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
              'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
    month = months[month]

    # Add the year and month as new columns
    df['year'] = year
    df['month'] = month

    # Write to a new Parquet file
    df.to_parquet(new_file_path)


def add_event_type(input_parquet_file, output_parquet_file):
    # Read the parquet file into a DataFrame
    df = pd.read_parquet(input_parquet_file)

    # Regular expression pattern to extract event type from filename
    pattern = r'_(\w+_event)_\d{4}-\d{2}-\d{2}_'

    # Extract event types using the pattern and add event type column
    df['event_type'] = df['filename'].str.extract(pattern)[0]

    # Write the updated DataFrame to the new Parquet file
    df.to_parquet(output_parquet_file, index=False)


def connect_to_motherduck(database):
    # Connect to MotherDuck
    token = key

    if token is None:
        raise ValueError("MotherDuck token not found in environment variables")

    connection_string = f'md:{database}?motherduck_token={token}'
    con = duckdb.connect(connection_string)
    return con


def load_into_duckdb(con, par_path):
    # Load to MotherDuck
    query = f"CREATE TABLE permit_2024 AS SELECT * FROM read_parquet('{par_path}')"
    try:
        con.execute(query)
    except Exception as e:
        print(f"An error occurred: {e}")


def cleanup_parquet_files(directory, file_extension='.parquet'):
    # Iterate over all files in the directory
    for file in os.listdir(directory):
        if file.endswith(file_extension):
            os.remove(os.path.join(directory, file))


def log_completion():
    # Specify the directory name
    log_dir = "log_files"
    # Create the directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    log_file_path = os.path.join(log_dir, "sm_permit_completion_log.txt")

    # Write a log for each complete run
    with open(log_file_path, "a") as log_file:
        now = datetime.datetime.now()
        log_file.write(f"SM data script completed on {now.strftime('%Y-%m-%d %H:%M:%S')}\n")


def main():

    # Step 1: Create initial Parquet file
    parquet_1 = create_parquet('sm_permit/year_month.json')

    print("Parquet Created")

    # Step 2: Process JSON files into a Parquet file
    process_json(source_data, parquet_1)

    print("json files processed into 1st parquet file")

    # Step 3: Update date in Parquet file (if necessary)
    update_date(parquet_1, "updated_date.parquet")

    print("parquet update 1 - month and year")

    # Step 4: Add event type to Parquet file (if necessary)
    add_event_type("updated_date.parquet", "updated_event_type.parquet")

    print("parqeuet update 2 - event type")

    # Step 5: Connect to DuckDB
    con = connect_to_motherduck(db)

    print("motherduck database connection made")

    # Step 6: Load Parquet file into DuckDB
    load_into_duckdb(con, "updated_event_type.parquet")

    print("motherduck data load complete")

    # Step 7: Cleanup tasks
    # Delete parquet files
    cleanup_parquet_files(".")

    print("cleanup complete")

    # Step 8: Log completion
    log_completion()

    print("process 100% complete")


if __name__ == "__main__":
    main()

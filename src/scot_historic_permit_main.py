from general_functions.create_motherduck_connection import connect_to_motherduck
from general_functions.get_creds import get_secrets
from general_functions.creds import secret_name
from scottish_permit_functions.generate_dl_links import dl_link_creator
from scottish_permit_functions.experimental_extract_load_data import fetch_presigned_url, fetch_data, process_batches


def main(schema_name, batch_limit):
    
    # Credentials for MotherDuck
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    schema = secrets[schema_name]
    
    conn = connect_to_motherduck(token, database)
    url = dl_link_creator("14")
    dl_url = fetch_presigned_url(url)
    dl_data = fetch_data(dl_url)
    process_batches(dl_data, batch_limit, conn, schema)
    print("DONE")

if __name__ == "__main__":
    main("scot_schema_24", 50000)
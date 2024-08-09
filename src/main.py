from .geoplace_swa_codes.create_table_name import get_table_name
from .geoplace_swa_codes.fetch_swa_codes import get_link, fetch_swa_codes, validate_data_model
from .geoplace_swa_codes.load_swa_codes import create_table_swa_codes_motherduck, load_swa_code_data_motherduck

from .general_functions.create_motherduck_connection import connect_to_motherduck
from .general_functions.creds import secret_name
from .general_functions.get_credentials import get_secrets

def main():
    
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    
    conn = connect_to_motherduck(token, database)
    
    link = get_link()
    table_name = get_table_name(link)
    
    swa_code_df = fetch_swa_codes()
    validate_data_model()
    
    create_table_swa_codes_motherduck(conn, table_name)
    load_swa_code_data_motherduck(swa_code_df, conn, table_name)

if __name__ == "__main__":
    main()

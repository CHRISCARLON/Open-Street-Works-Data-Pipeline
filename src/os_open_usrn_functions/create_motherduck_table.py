def create_table(conn, schema): 
    
    if conn:
        try:
            table_command = f"""CREATE OR REPLACE TABLE "{schema}"."open_usrns_latest" (
                geometry VARCHAR,
                street_type VARCHAR, 
                usrn BIGINT
            );"""
            conn.execute(table_command)
            print("MotherDuck Table created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

def motherduck_create_table(conn, schema, table):
    """
    This function creates a database table based on the StreetManager Permit data schema
    It takes a connection object, a schema name, and a table name
    Please note that the schema should already EXIST in the database
    
    """
    if conn:
        try:
            table_command = f"""CREATE TABLE "{schema}"."{table}" (
                version BIGINT,
                event_reference BIGINT,
                event_type VARCHAR,
                event_time VARCHAR,
                object_type VARCHAR,
                object_reference VARCHAR,
                work_reference_number VARCHAR,
                work_category VARCHAR,
                work_category_ref VARCHAR,
                work_status VARCHAR,
                work_status_ref VARCHAR,
                activity_type VARCHAR,
                permit_reference_number VARCHAR,
                permit_status VARCHAR,
                permit_conditions VARCHAR,
                collaborative_working VARCHAR,
                collaboration_type VARCHAR,
                collaboration_type_ref VARCHAR,
                promoter_swa_code VARCHAR,
                promoter_organisation VARCHAR,
                highway_authority VARCHAR,
                highway_authority_swa_code VARCHAR,
                works_location_coordinates VARCHAR,
                works_location_type VARCHAR,
                town VARCHAR,
                street_name VARCHAR,
                usrn VARCHAR,
                road_category VARCHAR,
                area_name VARCHAR,
                traffic_management_type VARCHAR,
                traffic_management_type_ref VARCHAR,
                current_traffic_management_type VARCHAR,
                current_traffic_management_type_ref VARCHAR,
                current_traffic_management_update_date VARCHAR,
                proposed_start_date VARCHAR,
                proposed_start_time VARCHAR,
                proposed_end_date VARCHAR,
                proposed_end_time VARCHAR,
                actual_start_date_time VARCHAR,
                actual_end_date_time VARCHAR,
                is_ttro_required VARCHAR,
                is_covid_19_response VARCHAR,
                is_traffic_sensitive VARCHAR,
                is_deemed VARCHAR,
                close_footway VARCHAR,
                close_footway_ref VARCHAR
            );"""
            conn.execute(table_command)
            print("MotherDuck Table created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

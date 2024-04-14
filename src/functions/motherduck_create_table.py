def motherduck_create_table(conn, schema, table):
    if conn:
        try:
            table_command = f"""CREATE TABLE "{schema}"."{table}" (
                event_reference BIGINT,
                event_type VARCHAR,
                work_reference_number VARCHAR,
                permit_reference_number VARCHAR,
                promoter_swa_code VARCHAR,
                promoter_organisation VARCHAR,
                highway_authority VARCHAR,
                works_location_coordinates VARCHAR,
                street_name VARCHAR,
                area_name VARCHAR,
                work_category VARCHAR,
                traffic_management_type VARCHAR,
                proposed_start_date VARCHAR,
                proposed_start_time VARCHAR,
                proposed_end_date VARCHAR,
                proposed_end_time VARCHAR,
                actual_start_date_time VARCHAR,
                actual_end_date_time VARCHAR,
                work_status VARCHAR,
                usrn VARCHAR,
                highway_authority_swa_code VARCHAR,
                work_category_ref VARCHAR,
                traffic_management_type_ref VARCHAR,
                work_status_ref VARCHAR,
                activity_type VARCHAR,
                is_ttro_required VARCHAR,
                is_covid_19_response VARCHAR,
                works_location_type VARCHAR,
                permit_conditions VARCHAR,
                road_category VARCHAR,
                is_traffic_sensitive VARCHAR,
                is_deemed VARCHAR,
                permit_status VARCHAR,
                town VARCHAR,
                collaborative_working VARCHAR,
                close_footway VARCHAR,
                close_footway_ref VARCHAR,
                current_traffic_management_type VARCHAR,
                current_traffic_management_type_ref VARCHAR,
                current_traffic_management_update_date VARCHAR,
                event_time VARCHAR,
                object_type VARCHAR,
                object_reference VARCHAR,
                version BIGINT,
                collaboration_type VARCHAR,
                collaboration_type_ref VARCHAR
            );"""
            conn.execute(table_command)
            print("Table created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

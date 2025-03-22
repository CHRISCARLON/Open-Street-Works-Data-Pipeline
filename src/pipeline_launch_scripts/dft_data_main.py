import psutil
import os

from memory_profiler import profile
from loguru import logger

from dft_data.dft_data import fetch_gss_codes, fetch_road_lengths, fetch_traffic_flows
from general_functions.create_motherduck_connection import MotherDuckConnector
from src.auth.creds import secret_name
from src.auth.get_credentials import get_secrets
from general_functions.create_table_names import create_table_names


@profile
def main():
    """
    This will process dft road lengths, annual traffic flows, and dft la list to get ons gss codes
    """
    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss
    print(f"Initial Memory: {initial_memory}")

    logger.success("DFT DATA STARTED")

    # Fetch secret variables
    secrets = get_secrets(secret_name)

    token = secrets["motherduck_token"]
    database = secrets["motherdb"]

    # Create dft tables
    df = fetch_road_lengths()
    df_schema = "dft_road_lengths"
    df_table_name = create_table_names("dft_road_lengths")
    df_table_name_latest = "dft_road_lengths_latest"

    df_2 = fetch_gss_codes()
    df_2_schema = "dft_las_gss_code"
    df_2_table_name = create_table_names("dft_las_gss_code")
    df_2_table_name_latest = "dft_las_gss_code_latest"

    df_3 = fetch_traffic_flows()
    df_3_schema = "dft_traffic_flows"
    df_3_table_name = create_table_names("dft_traffic_flows")
    df_3_table_name_latest = "dft_traffic_flows_latest"

    # Create table and insert data into MotherDuck
    with MotherDuckConnector(token, database) as conn:
        # Road Lengths Tables
        create_road_lengths = f"""
        CREATE OR REPLACE TABLE "{df_schema}"."{df_table_name}" (
            ons_area_code VARCHAR,
            region VARCHAR,
            local_authority VARCHAR,
            trunk_motorways VARCHAR,
            principal_motorways VARCHAR,
            all_motorways VARCHAR,
            trunk_rural_a_roads VARCHAR,
            trunk_urban_a_roads VARCHAR,
            principal_rural_a_roads VARCHAR,
            principal_urban_a_roads VARCHAR,
            all_a_roads VARCHAR,
            major_trunk_roads VARCHAR,
            major_principal_roads VARCHAR,
            all_major_roads VARCHAR,
            rural_b_roads VARCHAR,
            urban_b_roads VARCHAR,
            rural_c_and_u_roads VARCHAR,
            urban_c_and_u_roads VARCHAR,
            minor_roads VARCHAR,
            total_road_length VARCHAR,
            notes VARCHAR
        );
        """
        conn.execute_query(create_road_lengths)

        create_road_lengths_latest = f"""
        CREATE OR REPLACE TABLE "{df_schema}"."{df_table_name_latest}" (
            ons_area_code VARCHAR,
            region VARCHAR,
            local_authority VARCHAR,
            trunk_motorways VARCHAR,
            principal_motorways VARCHAR,
            all_motorways VARCHAR,
            trunk_rural_a_roads VARCHAR,
            trunk_urban_a_roads VARCHAR,
            principal_rural_a_roads VARCHAR,
            principal_urban_a_roads VARCHAR,
            all_a_roads VARCHAR,
            major_trunk_roads VARCHAR,
            major_principal_roads VARCHAR,
            all_major_roads VARCHAR,
            rural_b_roads VARCHAR,
            urban_b_roads VARCHAR,
            rural_c_and_u_roads VARCHAR,
            urban_c_and_u_roads VARCHAR,
            minor_roads VARCHAR,
            total_road_length VARCHAR,
            notes VARCHAR
        );
        """
        conn.execute_query(create_road_lengths_latest)

        # GSS Codes Tables
        create_gss_codes = f"""
        CREATE OR REPLACE TABLE "{df_2_schema}"."{df_2_table_name}" (
            id VARCHAR,
            name VARCHAR,
            region_id VARCHAR,
            ita_id VARCHAR,
            ons_code VARCHAR,
        );
        """
        conn.execute_query(create_gss_codes)

        create_gss_codes_latest = f"""
        CREATE OR REPLACE TABLE "{df_2_schema}"."{df_2_table_name_latest}" (
            id VARCHAR,
            name VARCHAR,
            region_id VARCHAR,
            ita_id VARCHAR,
            ons_code VARCHAR,
        );
        """
        conn.execute_query(create_gss_codes_latest)

        # Traffic Flows Tables
        create_traffic_flows = f"""
        CREATE OR REPLACE TABLE "{df_3_schema}"."{df_3_table_name}" (
            local_authority_or_region_code VARCHAR,
            region VARCHAR,
            ita VARCHAR,
            local_authority VARCHAR,
            notes VARCHAR,
            units VARCHAR,
            "1993" VARCHAR,
            "1994" VARCHAR,
            "1995" VARCHAR,
            "1996" VARCHAR,
            "1997" VARCHAR,
            "1998" VARCHAR,
            "1999" VARCHAR,
            "2000" VARCHAR,
            "2001" VARCHAR,
            "2002" VARCHAR,
            "2003" VARCHAR,
            "2004" VARCHAR,
            "2005" VARCHAR,
            "2006" VARCHAR,
            "2007" VARCHAR,
            "2008" VARCHAR,
            "2009" VARCHAR,
            "2010" VARCHAR,
            "2011" VARCHAR,
            "2012" VARCHAR,
            "2013" VARCHAR,
            "2014" VARCHAR,
            "2015" VARCHAR,
            "2016" VARCHAR,
            "2017" VARCHAR,
            "2018" VARCHAR,
            "2019" VARCHAR,
            "2020" VARCHAR,
            "2021" VARCHAR,
            "2022" VARCHAR,
            "2023" VARCHAR
        );
        """
        conn.execute_query(create_traffic_flows)

        create_traffic_flows_latest = f"""
        CREATE OR REPLACE TABLE "{df_3_schema}"."{df_3_table_name_latest}" (
            local_authority_or_region_code VARCHAR,
            region VARCHAR,
            ita VARCHAR,
            local_authority VARCHAR,
            notes VARCHAR,
            units VARCHAR,
            "1993" VARCHAR,
            "1994" VARCHAR,
            "1995" VARCHAR,
            "1996" VARCHAR,
            "1997" VARCHAR,
            "1998" VARCHAR,
            "1999" VARCHAR,
            "2000" VARCHAR,
            "2001" VARCHAR,
            "2002" VARCHAR,
            "2003" VARCHAR,
            "2004" VARCHAR,
            "2005" VARCHAR,
            "2006" VARCHAR,
            "2007" VARCHAR,
            "2008" VARCHAR,
            "2009" VARCHAR,
            "2010" VARCHAR,
            "2011" VARCHAR,
            "2012" VARCHAR,
            "2013" VARCHAR,
            "2014" VARCHAR,
            "2015" VARCHAR,
            "2016" VARCHAR,
            "2017" VARCHAR,
            "2018" VARCHAR,
            "2019" VARCHAR,
            "2020" VARCHAR,
            "2021" VARCHAR,
            "2022" VARCHAR,
            "2023" VARCHAR
        );
        """
        conn.execute_query(create_traffic_flows_latest)

        # Insert data into dated tables
        insert_road_lengths = f"""
        INSERT INTO "{df_schema}"."{df_table_name}"
        SELECT * FROM df
        """
        conn.execute_query(insert_road_lengths)

        insert_gss_codes = f"""
        INSERT INTO "{df_2_schema}"."{df_2_table_name}"
        SELECT * FROM df_2
        """
        conn.execute_query(insert_gss_codes)

        insert_traffic_flows = f"""
        INSERT INTO "{df_3_schema}"."{df_3_table_name}"
        SELECT * FROM df_3
        """
        conn.execute_query(insert_traffic_flows)

        # Insert data into latest tables
        insert_road_lengths_latest = f"""
        INSERT INTO "{df_schema}"."{df_table_name_latest}"
        SELECT * FROM df
        """
        conn.execute_query(insert_road_lengths_latest)

        insert_gss_codes_latest = f"""
        INSERT INTO "{df_2_schema}"."{df_2_table_name_latest}"
        SELECT * FROM df_2

        """
        conn.execute_query(insert_gss_codes_latest)

        insert_traffic_flows_latest = f"""
        INSERT INTO "{df_3_schema}"."{df_3_table_name_latest}"
        SELECT * FROM df_3
        """
        conn.execute_query(insert_traffic_flows_latest)

        # Get the final memory usage
        final_memory = psutil.Process(os.getpid()).memory_info().rss
        print(final_memory)

        logger.success("DFT DATA PROCESSED")


if __name__ == "__main__":
    main()

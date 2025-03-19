import duckdb
from loguru import logger
from typing import Dict, Optional
from data_sources.data_source_config import DataSourceConfig, DataSourceType, DataProcessorType

class MotherDuckManager:
    """
    Generic MotherDuck manager that handles connections and table operations
    based on data source configurations.
    
    This class combines connection management with table creation capabilities,
    allowing for a streamlined workflow when working with different data sources.
    """

    def __init__(self, token: str, database: str):
        """
        Initialize the MotherDuck manager.

        Args:
            token: MotherDuck authentication token
            database: Database name to connect to
        """
        self.token = token
        self.database = database
        self.connection = None
        self._table_templates = self._initialize_templates()

    def _initialize_templates(self) -> Dict[DataSourceType, Dict[str, str]]:
        """
        Initialize table templates for different data source types.
        
        Returns:
            Dictionary mapping data source types to their column definitions
        """
        return {
            DataSourceType.STREET_MANAGER: {
                "version": "BIGINT",
                "event_reference": "BIGINT",
                "event_type": "VARCHAR",
                "event_time": "VARCHAR",
                "object_type": "VARCHAR",
                "object_reference": "VARCHAR",
                "work_reference_number": "VARCHAR",
                "work_category": "VARCHAR",
                "work_category_ref": "VARCHAR",
                "work_status": "VARCHAR",
                "work_status_ref": "VARCHAR",
                "activity_type": "VARCHAR",
                "permit_reference_number": "VARCHAR",
                "permit_status": "VARCHAR",
                "permit_conditions": "VARCHAR",
                "collaborative_working": "VARCHAR",
                "collaboration_type": "VARCHAR",
                "collaboration_type_ref": "VARCHAR",
                "promoter_swa_code": "VARCHAR",
                "promoter_organisation": "VARCHAR",
                "highway_authority": "VARCHAR",
                "highway_authority_swa_code": "VARCHAR",
                "works_location_coordinates": "VARCHAR",
                "works_location_type": "VARCHAR",
                "town": "VARCHAR",
                "street_name": "VARCHAR",
                "usrn": "VARCHAR",
                "road_category": "VARCHAR",
                "area_name": "VARCHAR",
                "traffic_management_type": "VARCHAR",
                "traffic_management_type_ref": "VARCHAR",
                "current_traffic_management_type": "VARCHAR",
                "current_traffic_management_type_ref": "VARCHAR",
                "current_traffic_management_update_date": "VARCHAR",
                "proposed_start_date": "VARCHAR",
                "proposed_start_time": "VARCHAR",
                "proposed_end_date": "VARCHAR",
                "proposed_end_time": "VARCHAR",
                "actual_start_date_time": "VARCHAR",
                "actual_end_date_time": "VARCHAR",
                "is_ttro_required": "VARCHAR",
                "is_covid_19_response": "VARCHAR",
                "is_traffic_sensitive": "VARCHAR",
                "is_deemed": "VARCHAR",
                "close_footway": "VARCHAR",
                "close_footway_ref": "VARCHAR"
            }
            # Add more templates for other data source types as needed
        }

    def connect(self) -> Optional[duckdb.DuckDBPyConnection]:
        """
        Establish a connection to MotherDuck.
        
        Returns:
            DuckDB connection object or None if connection fails
        """
        if not self.token:
            logger.warning("No token provided, MotherDuck connection not made")
            return None
            
        try:
            connection_string = f"md:{self.database}?motherduck_token={self.token}"
            self.connection = duckdb.connect(connection_string)
            logger.success("MotherDuck Connection Made")
            return self.connection
        except (duckdb.ConnectionException, duckdb.Error) as e:
            logger.warning(f"An error occurred with MotherDuck: {e}")
            raise e

    def create_table(self, schema: str, table: str, columns: Dict[str, str]) -> bool:
        """
        Create a table in MotherDuck with specified schema and columns.
        
        Args:
            schema: Schema name (must already exist)
            table: Table name to create
            columns: Dictionary of column names and their types
            
        Returns:
            Boolean indicating success
        """
        if self.connection is None:
            self.connect()
            
        if not self.connection:
            return False
            
        # Build column definitions from dictionary
        column_defs = ",\n                ".join([f"{col_name} {col_type}" for col_name, col_type in columns.items()])
        logger.info(f"Creating table {schema}.{table} with columns: {column_defs}")
        
        try:
            table_command = f"""CREATE OR REPLACE TABLE "{schema}"."{table}" (
                {column_defs}
            );"""
            self.connection.execute(table_command)
            logger.success(f"MotherDuck table '{schema}.{table}' created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def create_table_from_data_source(self, config: DataSourceConfig) -> bool:
        """
        Create tables based on a data source configuration.
        
        Args:
            config: DataSourceConfig object containing schema and table information
            
        Returns:
            Boolean indicating success
        """
        # Check if processor type is MotherDuck
        if config.processor_type != DataProcessorType.MOTHERDUCK:
            logger.warning(f"Data source configured for processor type {config.processor_type}, not MotherDuck")
            return False
            
        # Get schema name from config
        schema = config.schema_name
        
        # Check if we have a template for this data source type
        if config.source_type not in self._table_templates:
            logger.error(f"No table template found for data source type {config.source_type}")
            return False
            
        # Get column definitions for this data source type
        columns = self._table_templates[config.source_type]
        
        # Create tables for each table name in the config
        success = True
        for table_name in config.table_names:
            try:
                table_success = self.create_table(schema, table_name, columns)
                if not table_success:
                    success = False
            except Exception as e:
                logger.error(f"Failed to create table {schema}.{table_name}: {e}")
                success = False
                
        return success

    def create_schema_if_not_exists(self, schema: str) -> bool:
        """
        Create a schema if it doesn't already exist.
        
        Args:
            schema: Schema name to create
            
        Returns:
            Boolean indicating success
        """
        if self.connection is None:
            self.connect()
            
        if not self.connection:
            return False
            
        try:
            self.connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
            logger.success(f"Schema '{schema}' created or already exists")
            return True
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            raise

    def setup_for_data_source(self, config: DataSourceConfig):
        """
        Complete setup for a data source - create schema and tables.
        
        Args:
            config: DataSourceConfig object
        """
        # Create schema
        self.create_schema_if_not_exists(config.schema_name)
        self.create_table_from_data_source(config)

    def close(self):
        """Close the MotherDuck connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("MotherDuck Connection Closed")

    def __enter__(self):
        """Context manager entry point."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point."""
        self.close()

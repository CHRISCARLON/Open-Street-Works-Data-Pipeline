from typing import Optional, List, Union
from datetime import date, timedelta, datetime
from data_source_config import DataProcessorType, DataSourceType, TimeRange, DataSourceConfig

class StreetManager(DataSourceConfig):
    """
    Configuration class for Street Manager data source.
    Implements the DataSourceConfigProtocol.
    """
    
    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
        year: Optional[int] = None,
        start_month: Optional[int] = None,
        end_month: Optional[int] = None,
    ):
        """
        Initialise a Street Manager configuration.
        
        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data
            batch_limit: Optional limit for batch processing
            year: Specific year for historic data (defaults to previous year)
            start_month: Starting month for historic data (1-12, defaults to 1)
            end_month: Ending month for historic data (non-inclusive, 1-13, defaults to 13)
        """
        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.STREET_MANAGER
        
        # Set default values for year and month range
        self.year = year if year is not None else datetime.now().year - 1
        self.start_month = start_month if start_month is not None else 1
        self.end_month = end_month if end_month is not None else 13
    
    @property
    def processor_type(self) -> DataProcessorType:
        return self._processor_type
    
    @property
    def source_type(self) -> DataSourceType:
        return self._source_type
    
    @property
    def time_range(self) -> TimeRange:
        return self._time_range
    
    @property
    def base_url(self) -> str:
        """Get the base URL for the configured data source."""
        return self.source_type.base_url
    
    @property
    def download_links(self) -> list[str]:
        """
        Get the download links for Street Manager data.
        
        Returns:
            A list of download links based on the time range
        """
        base_url = self.base_url.rstrip('/')
        
        if self.time_range == TimeRange.LATEST:
            # For latest data, generate the last month's link directly
            current_date = date.today()
            first_day_of_current_month = date(current_date.year, current_date.month, 1)
            last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
            year = last_day_of_previous_month.year
            month = f"{last_day_of_previous_month.month:02d}"
            
            return [f"{base_url}/{year}/{month}.zip"]
        
        elif self.time_range == TimeRange.HISTORIC:
            # For historic data, use the specified year and month range
            links = []
            for month in range(self.start_month, self.end_month):
                month_formatted = f"{month:02d}"
                url = f"{base_url}/{self.year}/{month_formatted}.zip"
                links.append(url)
                
            return links
        
        # Default fallback
        return ["NO Download Links Generated"]

    def last_month(self) -> list:
        """
        Get the previous month's year and month.
        
        Returns:
            [2024, "03"] if you run it in April 2024
        """
        current_date = date.today()
        first_day_of_current_month = date(current_date.year, current_date.month, 1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        year = last_day_of_previous_month.year
        month = f"{last_day_of_previous_month.month:02d}"
        return [year, month]
    
    def date_for_table(self) -> Union[str, List[str]]:
        """
        Creates formatted date string(s) for table names based on time range.
        
        Returns:
            For LATEST: A string like "03_2024" for the previous month
            For HISTORIC: A list of strings like ["01_2023", "02_2023", ...] 
        """
        if self.time_range == TimeRange.LATEST:
            # Get previous month
            year_month = self.last_month()
            year = str(year_month[0])
            month = year_month[1]
            return f"{month}_{year}"
        
        elif self.time_range == TimeRange.HISTORIC:
            # Extract dates from generated download links
            table_names = []
            for link in self.download_links:
                parts = link.split('/')
                year = parts[-2]
                month = parts[-1].replace('.zip', '')
                table_names.append(f"{month}_{year}")
            return table_names
        
        else:
            raise ValueError("Invalid time range")
    
    @property
    def table_names(self) -> List[str]:
        """
        Get all table names when multiple historic tables are available.
        """
        
        date_suffix = self.date_for_table()
        
        if isinstance(date_suffix, str):
            # Single table case
            return [f"{date_suffix}"]
        
        elif isinstance(date_suffix, list):
            # Multiple tables case
            return [f"{suffix}" for suffix in date_suffix]
        
        else:
            raise ValueError("Invalid date suffix")
    
    @property
    def schema_name(self) -> str:
        """
        Get the schema name for the Street Manager data based on last month.
        """
        if self.time_range == TimeRange.LATEST:
            year_month = self.last_month()
            previous_month_year = year_month[0]
            return f"raw_data_{previous_month_year}"
        elif self.time_range == TimeRange.HISTORIC:
            return f"raw_data_{self.year}"
        return f"raw_data_{date.today().year}"
    
    def __str__(self) -> str:
        """String representation of the configuration."""
        links_str = ", ".join(self.download_links[:2])
        if len(self.download_links) > 2:
            links_str += f", ... ({len(self.download_links)} total)"
            
        return (f"StreetManagerConfig(processor={self.processor_type.value}, "
                f"source={self.source_type.code}, "
                f"base_url={self.base_url}, "
                f"time_range={self.time_range.value}, "
                f"batch_limit={self.batch_limit}, "
                f"download_links=[{links_str}]), "
                f"schema_name={self.schema_name}, "
                f"table_names={self.table_names}")

    @classmethod
    def create_default_latest(cls) -> 'StreetManager':
        """Create a default Street Manager configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=150000
        )
    
    @classmethod
    def create_default_historic_2024(cls) -> 'StreetManager':
        """Create a default Street Manager configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=150000,
            year=2024,
            start_month=1,
            end_month=13
        )
    
    @classmethod
    def create_default_historic_2023(cls) -> 'StreetManager':
        """Create a default Street Manager configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=150000,
            year=2023,
            start_month=1,
            end_month=13
        )

    @classmethod
    def create_default_historic_2022(cls) -> 'StreetManager':
        """Create a default Street Manager configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=150000,
            year=2022,
            start_month=1,
            end_month=13
        )

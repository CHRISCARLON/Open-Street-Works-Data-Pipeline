from typing import Optional
from datetime import date, timedelta, datetime
from data_sources.data_source_config import DataProcessorType, DataSourceType, TimeRange, DataSourceConfig

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
        end_month: Optional[int] = None
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
                f"download_links=[{links_str}])")
    
    @classmethod
    def create_default(cls) -> 'StreetManager':
        """Create a default Street Manager configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=150000
        )

# Example usage
street_manager_config = StreetManager.create_default()
print(street_manager_config)
print(f"Download links: {street_manager_config.download_links}")

# street_manager_historic_config = StreetManager(
#     processor_type=DataProcessorType.MOTHERDUCK,
#     time_range=TimeRange.HISTORIC,
#     batch_limit=150000,
#     year=2024,
#     start_month=1,
#     end_month=13
# )
# print(street_manager_historic_config)


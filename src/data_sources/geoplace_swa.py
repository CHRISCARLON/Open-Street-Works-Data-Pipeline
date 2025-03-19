from typing import Optional
from data_source_config import DataProcessorType, DataSourceType, TimeRange, DataSourceConfig
import requests
from bs4 import BeautifulSoup, Tag
from loguru import logger

class GeoplaceSwa(DataSourceConfig):
    """
    Configuration class for Geoplace SWA data source.
    Implements the DataSourceConfigProtocol.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
    ):
        """
        Initialise a Geoplace SWA configuration.
        
        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data
            batch_limit: Optional limit for batch processing
            source_type: The type of data source
        """

        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.GEOPLACE_SWA

    
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
        """Get the download links for the configured data source."""
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            download_link = soup.find('a', class_='download-item__download-link')
            if download_link and isinstance(download_link, Tag):
                href = download_link.get('href')
                logger.success("Link Found")
                return [str(href)]
            else:
                raise ValueError("No valid download link found on the page")
        except Exception as e:
            logger.error(f"Error in get_link: {e}")
            raise ValueError(f"Failed to get download link: {e}")
    
    @property
    def schema_name(self) -> str:
        """Get the schema name for the configured data source."""
        return "geoplace_swa"
    
    @property
    def table_names(self) -> list[str]:
        """Get the table names for the configured data source."""
        return ["swa_codes"]
    
    def __str__(self) -> str:
        return (f"Geoplace SWA Configuration: "
                f"processor={self.processor_type.value}, "
                f"source={self.source_type.code}, "
                f"base_url={self.base_url}, "
                f"time_range={self.time_range.value}, "
                f"batch_limit={self.batch_limit}, "
                f"download_links={self.download_links}, "
                f"schema_name={self.schema_name}, "
                f"table_names={self.table_names}")
        

config = GeoplaceSwa(
    processor_type=DataProcessorType.MOTHERDUCK,
    time_range=TimeRange.LATEST,
    batch_limit=150000
)

print(config.download_links)


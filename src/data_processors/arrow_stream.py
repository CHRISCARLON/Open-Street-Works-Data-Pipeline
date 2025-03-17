# TODO: Move this to a separate class?

import requests
import json
import pyarrow as pa
from stream_unzip import stream_unzip
from data_sources.street_manager import StreetManager
from tqdm import tqdm

def stream_unzip_to_arrow(dl_url, max_size_mb=5):
    """
    Stream and unzip the first few MB of data from a download link into an Arrow table.
    
    Args:
        dl_url: URL for the zipped data file
        max_size_mb: Maximum size to download in MB (default: 5)
    
    Returns:
        PyArrow Table containing the unzipped data
    """
    all_data = []
    max_bytes = max_size_mb * 1024 * 1024
    
    # First, make a HEAD request to get the content length if available
    with requests.head(dl_url, timeout=10) as head_response:
        head_response.raise_for_status()
        total_size = int(head_response.headers.get('content-length', 0))
        
    # Define a generator function to stream the data with progress bar
    def zipped_chunks():
        with requests.get(dl_url, stream=True, timeout=30) as response:
            response.raise_for_status()  # Ensure we got a valid response
            
            # Create progress bar for download
            with tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                desc="Downloading",
                leave=True
            ) as pbar:
                for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks
                    pbar.update(len(chunk))
                    yield chunk
    
    # Process the downloaded chunks through stream_unzip
    file_count = 0
    total_bytes = 0
    
    # Create a progress bar for the unzipped data size
    with tqdm(
        total=max_bytes,
        unit='B',
        unit_scale=True,
        desc="Processing",
        leave=True
    ) as process_pbar:
        
        for file, size, unzipped_chunks in stream_unzip(zipped_chunks()):
            try:
                # Join the unzipped chunks and parse as JSON
                bytes_obj = b''.join(unzipped_chunks)
                bytes_size = len(bytes_obj)
                total_bytes += bytes_size
                
                # Update the processing progress bar
                process_pbar.update(bytes_size)
                
                json_data = json.loads(bytes_obj.decode('utf-8'))
                
                # Flatten the JSON structure
                flattened = flatten_json(json_data)
                all_data.append(flattened)
                
                file_count += 1
                
                # Stop if we've processed enough data
                if total_bytes >= max_bytes:
                    print(f"\nReached size limit of {max_size_mb}MB after processing {file_count} files")
                    break
                    
            except Exception as e:
                print(f"\nError processing file {file}: {e}")
    
    print(f"Processed {file_count} files ({total_bytes/1024/1024:.2f}MB uncompressed)")
    
    # Convert the collected data to a PyArrow table
    if not all_data:
        return pa.table([])
    
    # Convert to PyArrow table
    table = pa.Table.from_pylist(all_data)
    print(f"Created Arrow table with {len(table)} rows and {len(table.column_names)} columns")
    return table


def flatten_json(json_obj, parent_key='', sep='.'):
    """
    Flatten a nested JSON object into a single level dictionary.
    
    Args:
        json_obj: The JSON object to flatten
        parent_key: The base key for nested objects
        sep: Separator for nested keys
        
    Returns:
        A flattened dictionary
    """
    items = {}
    for k, v in json_obj.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_json(v, new_key, sep=sep))
        elif isinstance(v, list):
            items[new_key] = str(v)
        else:
            items[new_key] = v     
    return items


def preview_street_manager_data(config=None, max_size_mb=5):
    """
    Preview Street Manager data by streaming a small portion into an Arrow table.
    
    Args:
        config: StreetManager configuration (optional, uses default if None)
        max_size_mb: Maximum size to download in MB
        
    Returns:
        PyArrow Table with the preview data
    """
    # Use provided config or create default
    if config is None:
        config = StreetManager.create_default()
    
    # Get the download links from the config
    download_links = config.download_links
    
    if not download_links or download_links[0] == "NO Download Links Generated":
        raise ValueError("No valid download links available in the configuration")
    
    # Use the first link for preview (usually the most recent data)
    first_link = download_links[0]
    print(f"Previewing data from: {first_link}")
    
    # Stream and convert to Arrow
    return stream_unzip_to_arrow(first_link, max_size_mb=max_size_mb)


# Example usage
if __name__ == "__main__":
    # Example 1: Using default config (latest month)
    table = preview_street_manager_data()
    print(f"Created Arrow table with {len(table)} rows and {len(table.column_names)} columns")

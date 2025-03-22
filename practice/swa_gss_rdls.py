import requests
import pandas as pd

from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup, Tag
from io import BytesIO
from loguru import logger
from msoffcrypto import OfficeFile
from typing import Optional, Tuple


def save_to_parquet(
    df: Optional[pd.DataFrame], data_type: str, base_path: str = "data"
) -> bool:
    if df is None:
        logger.error(f"Cannot save {data_type} codes - DataFrame is None")
        return False

    try:
        # Create directory if it doesn't exist
        save_path = Path(base_path)
        save_path.mkdir(parents=True, exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{data_type}_codes_{timestamp}.parquet"
        full_path = save_path / filename

        # Save to parquet
        df.to_parquet(full_path, index=False)
        logger.info(f"Successfully saved {data_type} codes to {full_path}")
        return True

    except Exception as e:
        logger.error(f"Error saving {data_type} codes to parquet: {e}")
        return False


def get_link() -> Optional[str]:
    """
    Scrape download link from website
    Returns:
        str: Download link as a string
    Raises:
        ValueError: If no download link is found or any error occurs
    """
    url = "https://www.geoplace.co.uk/local-authority-resources/street-works-managers/view-swa-codes"
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        download_link = soup.find("a", class_="download-item__download-link")
        if download_link and isinstance(download_link, Tag):
            href = download_link.get("href")
            logger.success("Link Found")
            return str(href)
        else:
            raise ValueError("No valid download link found on the page")
    except Exception as e:
        logger.error(f"Error in get_link: {e}")
        raise ValueError(f"Failed to get download link: {e}")


def fetch_swa_codes() -> Optional[pd.DataFrame]:
    """
    Use download link to fetch data.
    Data is an old xls file and needs extra steps to create dataframe.
    Calls the get_link function.

    Returns:
        Optional[pd.DataFrame]: DataFrame containing the SWA codes data, or None if an error occurs.
    """
    try:
        url = get_link()
        # Explicit type assertion
        assert isinstance(url, str), "URL must be a string"
    except (ValueError, AssertionError) as e:
        logger.error(f"Error getting download link: {e}")
        return None

    try:
        response = requests.get(url)
        response.raise_for_status()

        result = BytesIO(response.content)
        office_file = OfficeFile(result)
        office_file.load_key("VelvetSweatshop")

        decrypted_file = BytesIO()
        office_file.decrypt(decrypted_file)
        decrypted_file.seek(0)

        # Read in and do some basic renames and transformation
        df = pd.read_excel(decrypted_file, header=1, engine="xlrd")
        df = df.astype(str).replace("nan", None)
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("/", "_")

        # Add date time processed column
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["date_time_processed"] = current_time

        logger.success(f"DataFrame created successfully: {df.head(10)}")

        save_to_parquet(df, "swa")

        return

    except requests.RequestException as e:
        logger.error(f"Error downloading file: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in fetch_swa_codes: {e}")

    return None


def fetch_gss_codes() -> Optional[pd.DataFrame]:
    try:
        data = requests.get("https://roadtraffic.dft.gov.uk/api/local-authorities")
        reponse = data.json()
        df = pd.DataFrame(reponse)
        save_to_parquet(df, "gss")
        return
    except Exception:
        raise


def fetch_road_lengths():
    try:
        data = requests.get(
            "https://assets.publishing.service.gov.uk/media/65fb0052aa9b76001dfbdc03/rdl0202.ods"
        )
        response = data.content
        bytes_io = BytesIO(response)

        df = pd.read_excel(
            bytes_io,
            sheet_name="RDL0202a",
            skiprows=7,
        )
        save_to_parquet(df, "rdls")
        return df

    except Exception as e:
        raise Exception(f"Failed to fetch road length data: {str(e)}")


def clean_name(x: str):
    x = x.replace("LONDON BOROUGH OF", "").strip()
    x = x.replace("COUNCIL", "").strip()
    x = x.replace("COUNTY COUNCIL", "").strip()
    x = x.replace("COUNTY", "").strip()
    x = x.replace("BOROUGH COUNCIL", "").strip()
    x = x.replace("BOROUGH", "").strip()
    x = x.replace("CITY", "").strip()
    x = x.replace("CITY COUNCIL", "").strip()
    x = x.replace("METROPOLITAN", "").strip()
    x = x.replace("CITY OF", "").strip()
    x = x.replace("DISTRICT", "").strip()
    x = x.replace("ROYAL BOROUGH OF", "").strip()
    x = x.replace("CORPORATION", "").strip()
    x = x.replace("COUNCIL OF THE", "").strip()
    x = x.replace(", City of", "").strip()
    x = x.replace("City of", "").strip()
    x = x.replace(", County of", "").strip()
    x = x.replace("upon tyne", "").strip()
    x = x.replace("&", "and").strip()
    x = x.replace(",", "").strip()
    x = x.replace("excluding Isles of Scilly", "").strip()
    x = x.replace("Kingston upon", "").strip()
    x = str(x).lower()
    return x


def process_data() -> Optional[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
    df = pd.read_parquet("test_data/swa_codes_20241205_114253.parquet")
    df_2 = pd.read_parquet("test_data/gss_codes_20241205_114253.parquet")
    df_3 = pd.read_parquet("test_data/rdls_codes_20241205_172714.parquet")

    # Validate data
    if df is None or df_2 is None or df_3 is None:
        print("Error: One or both dataframes are None")
        return None

    try:
        df_swa = df[["account_name", "swa_code", "account_type"]]
        df_gss = df_2[["name", "ons_code", "id"]]
        df_rdls = df_3

        df_swa.loc[:, "account_name"] = df_swa.loc[:, "account_name"].apply(clean_name)
        df_gss.loc[:, "name"] = df_gss.loc[:, "name"].apply(clean_name)
        df_rdls = df_rdls.drop(columns=["Notes"])

        if not isinstance(df_swa, pd.DataFrame) or not isinstance(df_gss, pd.DataFrame):
            print("Error: Incorrect data types in processed data")
            return None

        return df_swa, df_gss, df_rdls

    except Exception as e:
        print(f"Error processing data: {e}")
        return None


def merge_datasets(
    data: Optional[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]],
) -> pd.DataFrame:
    if data is None:
        raise ValueError("Input data is None")

    df_swa, df_gss, df_rdls = data
    try:
        merge_1: pd.DataFrame = (
            df_swa.merge(df_gss, left_on="account_name", right_on="name", how="left")
            .drop(columns=["name"])
            .dropna(subset=["ons_code"])
            .loc[
                lambda df: df["account_type"].isin(
                    ["English Unitary", "English County"]
                )
            ]
        )

        merge_2: pd.DataFrame = merge_1.merge(
            df_rdls, left_on="ons_code", right_on="ONS Area Code", how="left"
        )

        merge_2 = merge_2.rename(
            columns={
                "account_name": "local_authority_name",
                "account_type": "local_authority_type",
                "id": "df_local_authority_id",
            }
        )

        return merge_2
    except Exception as e:
        print(f"Error during merge: {e}")
        raise


if __name__ == "__main__":
    data = process_data()
    merged = merge_datasets(data)
    print(merged)

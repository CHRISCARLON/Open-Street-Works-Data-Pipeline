import requests
import pandas as pd

from io import BytesIO
from typing import Optional

def fetch_road_lengths() -> Optional[pd.DataFrame]:
    try:
        data = requests.get("https://assets.publishing.service.gov.uk/media/65fb0052aa9b76001dfbdc03/rdl0202.ods")
        response = data.content
        bytes_io = BytesIO(response)

        df = pd.read_excel(
            bytes_io,
            sheet_name="RDL0202a",
            skiprows=7,
        )

        df = df.astype(str)
        df.columns = (df.columns
                     .str.replace("'", "")
                     .str.strip()
        )
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('/', '_')
        print(df.head(15))
        return df

    except Exception as e:
        raise Exception(f"Failed to fetch road length data: {str(e)}")

def clean_name(x: str):
    """
    Used to clean names columns ready for left join in the future
    """
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

def fetch_gss_codes() -> Optional[pd.DataFrame]:
    try:
        data = requests.get("https://roadtraffic.dft.gov.uk/api/local-authorities")
        reponse = data.json()

        df = pd.DataFrame(reponse)
        df = df.astype(str)
        df.columns = (df.columns
                     .str.lower()
                     .str.replace(' ', '_')
                     .str.replace('/', '_')
                     .str.strip())
        df.loc[:, "name"] = df.loc[:, "name"].apply(clean_name)
        print(df)
        return df
    except Exception as e:
        raise Exception(f"Failed to fetch gfss code data: {str(e)}")

def fetch_traffic_flows() -> Optional[pd.DataFrame]:
    try:
        data = requests.get("https://assets.publishing.service.gov.uk/media/664b86614f29e1d07fadcb4c/tra8904-km-by-local-authority.ods")
        response = data.content
        bytes_io = BytesIO(response)

        df = pd.read_excel(
            bytes_io,
            sheet_name="TRA8904",
            skiprows=4,
        )

        df = df.astype(str)
        df.columns = (df.columns
                    .str.replace(r'_\[note_8\]', '')
                    .str.replace(r'_\[note_8\]_\[r\]', '')
                    .str.replace('Integrated Transport Authority (ITA)', 'Integrated Transport Authority')
                    .str.strip()
        )
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('/', '_')
        print(df.head(15))
        print(df.columns)
        return df

    except Exception as e:
        raise Exception(f"Failed to fetch road length data: {str(e)}")

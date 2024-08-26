import pandas as pd

from src.geoplace_swa_codes.fetch_swa_codes import get_link, fetch_swa_codes

def test_get_link():
    """
    Assert that the link is actually the download link required.

    Will contain xls if successful.

    """
    link = get_link()
    assert link, "Link should not be None"
    assert "xls" in link

def test_fetch_swa_codes():
    swa_codes = fetch_swa_codes()
    assert isinstance(swa_codes, pd.DataFrame)
    assert not swa_codes.empty

    expected_columns = ['swa_code', 'account_name', 'prefix', 'ofgem_gas_licence', 'ofgem_electricity_licence', 'ofcom_licence', 'ofwat_licence']
    for col in expected_columns:
        assert col in swa_codes.columns, f"Column '{col}' should be present in the DataFrame"

    assert len(swa_codes) > 150

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional


class SWACodeModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    swa_code: Optional[str] = Field(None, alias="SWA Code")
    account_name: Optional[str] = Field(None, alias="Account Name")
    prefix: Optional[str] = Field(None, alias="Prefix")
    account_type: Optional[str] = Field(None, alias="Account Type")
    registered_for_street_manager: Optional[str] = Field(
        None, alias="Registered for Street Manager"
    )
    account_status: Optional[str] = Field(None, alias="Account Status")
    companies_house_number: Optional[str] = Field(None, alias="Companies House Number")
    previous_company_names: Optional[str] = Field(None, alias="Previous Company Names")
    linked_parent_company: Optional[str] = Field(None, alias="Linked/Parent Company")
    website: Optional[str] = Field(None, alias="Website")
    plant_enquiries: Optional[str] = Field(None, alias="Plant Enquiries")
    ofgem_electricity_licence: Optional[str] = Field(
        None, alias="Ofgem Electricity Licence"
    )
    ofgem_gas_licence: Optional[str] = Field(None, alias="Ofgem Gas Licence")
    ofcom_licence: Optional[str] = Field(None, alias="Ofcom Licence")
    ofwat_licence: Optional[str] = Field(None, alias="Ofwat Licence")
    company_subsumed_by: Optional[str] = Field(None, alias="Company Subsumed By")
    swa_code_of_new_company: Optional[str] = Field(
        None, alias="SWA Code of New Company"
    )

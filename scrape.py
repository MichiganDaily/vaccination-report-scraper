"""Scrape UM tableau for daily vaccination percentage."""

from tableauscraper import TableauScraper as TS
from datetime import datetime
import json
import requests
import pandas as pd

url = "https://tableau.dsc.umich.edu/t/UM-Public/views/U-MCovid19StudentVaccine/PUBLICThermometer"

ts = TS()
ts.loads(url)

students = ts.getWorksheet("Therm PUBLIC Stud (ALL Status)")
students = students.data[students.data["Verification Grouping-alias"].eq("Fully Vaccinated")].rename(
    columns={
        "Max Admin Date (today)-alias": "date",
        "AGG(Vaccinated- Percent (PUBLIC all status))-value": "percent_vaccinated",
        # "ATTR(Vaccinated- Percent (92 FIXED PUBLIC))-alias": "percent_vaccinated"
    },
).loc[:, ["date", "percent_vaccinated"]]
students.loc[:, "group"] = "Students"
students.loc[:, "total_count"] = 50000  # based on preliminary enrollment data

employees = ts.getWorksheet("Fac/Staff Therm (PUBLIC all status)")
employees = employees.data[employees.data["Verification Grouping-alias"].eq("Fully Vaccinated")].rename(
    columns={
        "Fac / (Staff + Temp) (group)-value": "group",
        "CNTD(EMPLID)-alias": "total_count",
        "Max Admin Date (today)-alias": "date",
        "AGG(Vaccinated- Percent (all status))-value": "percent_vaccinated",
    },
).loc[:, ["group", "total_count", "date", "percent_vaccinated"]]

washtenaw = ts.getWorksheet("WashCounty Data (2)")
try:
    washtenaw = washtenaw.data.rename(
        columns={
            "Week Ending Date-alias": "date",
            "SUM(Census Mi Population 12 Years And Older)-alias": "total_count",
            "AGG(Percent Coverage)-alias": "percent_vaccinated",
        },
    ).loc[:, ["date", "total_count", "percent_vaccinated"]]
    washtenaw.loc[:, "group"] = "Washtenaw"
except:
    print('Error getting Washtenaw data.')

all_empl = ts.getWorksheet("Employee Therm (PUBLIC all status)")
all_empl = all_empl.data[all_empl.data["Verification Grouping-alias"].eq("Fully Vaccinated")].rename(
    columns={
        "CNTD(EMPLID)-alias": "total_count",
        '"Employee"-value': "group",
        "Max Admin Date (today)-alias": "date",
        "AGG(Fully Vaccinated As of Today)-alias": "count_vaxxed",
        "AGG(Vaccinated- Percent (all status))-value": "percent_vaccinated",
    },
).loc[:, ["group", "total_count", "date", "percent_vaccinated", "count_vaxxed"]]

data = pd.concat([students, employees]).set_index("group")
data.loc[:, "num_vaccinated"] = (
    data["total_count"] * data["percent_vaccinated"]
).astype(int)
print(data)

output = {}
output["retrieved_timestamp"] = datetime.utcnow().isoformat()
output["vaccination_um_data"] = data.to_dict(orient="index")
try:
    risk_data = requests.get(
        "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=integrated_county_latest_external_data"
    ).json()
    county = [
        county
        for county in risk_data["integrated_county_latest_external_data"]
        if county["fips_code"] == 26161
    ][0]
    output["county_cdc_data"] = county
except:
    print("Error getting risk data.")

json.dump(output, open("daily_covid_data.json", "w"))

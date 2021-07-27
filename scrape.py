"""Scrape UM tableau for daily vaccination percentage."""

from tableauscraper import TableauScraper as TS
import pandas as pd

url = "https://tableau.dsc.umich.edu/t/UM-Public/views/U-MCovid19StudentVaccine/PUBLICThermometerbyTerm"

ts = TS()
ts.loads(url)

um = ts.getWorksheet("Thermometer- PUBLIC")
um.data.rename(
    columns={
        "ATTR(Max Date)-alias": "date",
        "AGG(Vaccinated- Percent (PUBLIC))-value": "percent_vaccinated-um",
    },
    inplace=True,
)

queue = ts.getWorksheet("Thermometer- PUBLIC IN QUEUE")
queue.data.rename(
    columns={
        "AGG(zAccessible- Vaccinated % (PUBLIC) In queue)-alias": "percent_waiting-um",
        "ATTR(Max Date)-alias": "waiting-date",
        "CNTD(UMID)-alias": "count_waiting-um",
    },
    inplace=True,
)

washtenaw = ts.getWorksheet("WashCounty Data")
washtenaw.data.rename(
    columns={
        "Week Ending Date-alias": "washtenaw-date",
        "SUM(MAX Sum Number of Doses)-alias": "num_doses-washtenaw",
        "SUM(Census Mi Population 12 Years And Older)-alias": "population-washtenaw",
        "AGG(Percent Coverage)-alias": "percent_vaccinated-washtenaw",
    },
    inplace=True,
)

data = pd.concat(
    [
        um.data.loc[:, ["date", "percent_vaccinated-um"]],
        queue.data,
        washtenaw.data,
    ],
    axis=1,
)

data.to_csv("./daily_percentage.csv", index=False)

try:
    df = pd.read_csv("./timeseries_percentage.csv")
    df = df.append(data)
except:
    data.to_csv("./timeseries_percentage.csv", index=False)

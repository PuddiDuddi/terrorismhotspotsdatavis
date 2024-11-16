import pandas as pd


def clean_data():
    print("Cleaning data...")
    df = pd.read_csv("terrorismcsv.csv")
    pd.set_option("display.max_columns", None)
    df[["imonth", "iday"]] = df[["imonth", "iday"]].replace(0, 1)
    df["date"] = pd.to_datetime(
        df[["iyear", "imonth", "iday"]].rename(
            columns={"iyear": "year", "imonth": "month", "iday": "day"}
        )
    )
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    usefuldata = df[
        [
            "eventid",
            "date",
            "country_txt",
            "region_txt",
            "provstate",
            "city",
            "latitude",
            "longitude",
            "success",
            "suicide",
            "attacktype1_txt",
            "targtype1_txt",
            "gname",
            "weapsubtype1_txt",
            "nkill",
            "nkillter",
            "nwound",
        ]
    ]
    usefuldata.loc[:, ["nkill", "nkillter", "nwound"]] = usefuldata.loc[
        :, ["nkill", "nkillter", "nwound"]
    ].fillna(0)
    usefuldata.loc[:, ["weapsubtype1_txt", "provstate", "city"]] = usefuldata.loc[
        :, ["weapsubtype1_txt", "provstate", "city"]
    ].fillna("Unknown")
    usefuldata = usefuldata.fillna("null")
    usefuldata.to_csv("filteredterroristdata.csv", index=False)
    print("Data cleaning completed.")

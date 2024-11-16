import pandas as pd
from loguru import logger

logger.remove()  # Remove default handler
logger.add(
    sink=lambda msg: print(msg),
    format="<level>{message}</level>",
    colorize=True,
    level="INFO",
)
logger.add("datacleaninglog.log", rotation="5 MB")


def clean_data():
    try:
        logger.info("Starting data cleaning process...")
        df = pd.read_csv("terrorismcsv.csv", low_memory=False)
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
        logger.success("✓ Data cleaning")
    except Exception as e:
        logger.error(f"✗ Data cleaning - Failed: {str(e)}")

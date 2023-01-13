import pandas as pd
import numpy as np


def get_random_road_type(x, road_type_not_nan):
    if not isinstance(x, str):
        return np.random.choice(road_type_not_nan["road_type"])
    return x


def get_mode_weather_conditions(x, mode, month):
    if not isinstance(x, str) & x.month == month:
        return mode
    return x["weather_conditions"]


def prepare_and_clean_df(df):
    df["date"] = pd.to_datetime(df["date"], dayfirst=True)
    df["month"] = pd.DatetimeIndex(df["date"]).month
    df.dropna(
        subset=["location_easting_osgr", "location_northing_osgr", "longitude", "latitude"],
        inplace=True,
    )
    road_type_not_nan = df[(df["road_type"].notna())]

    df["road_type"] = df["road_type"].apply(get_random_road_type, args=(road_type_not_nan,))
    for i in range(1, 12):
        month_mode = df.query("month == " + str(i))["weather_conditions"].mode()[0]
        df["new_weather_conditions"] = df.apply(
            lambda row: month_mode
            if (not isinstance(row["weather_conditions"], str) & row["month"] == i)
            else row["weather_conditions"],
            axis=1,
        )
    return df

import pandas as pd


def perform_enconding(df):
    dummies = pd.get_dummies(df["urban_or_rural_area"])
    df = pd.concat([df, dummies], axis=1).reindex(df.index)
    dummies = pd.get_dummies(df["accident_severity"])
    df = pd.concat([df, dummies], axis=1).reindex(df.index)
    return df


def get_season_from_month(month):
    if (month >= 1 and month <= 2) or month == 12:
        return "Winter"
    if month >= 3 and month <= 5:
        return "Spring"
    if month >= 6 and month <= 8:
        return "Summer"
    if month >= 9 and month <= 11:
        return "Fall"


def was_it_in_the_weekend(day):
    if day == "Saturday" or day == "Sunday":
        return 1
    return 0


def add_columns(df):
    df["week_number"] = df["date"].dt.week
    df["season"] = df["month"].apply(get_season_from_month)
    df["is_Weekend"] = df["day_of_week"].apply(was_it_in_the_weekend)
    return df 

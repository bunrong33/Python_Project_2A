import pandas as pd
import requests


def fetch_power(lat, lon, start_year, end_year, temporal="daily", PARAMS=None):
    """
    Fetch NASA POWER data for one (lat, lon) point.
    Parameters:
    lat : float
        Latitude in decimal degrees.
    lon : float
        Longitude in decimal degrees.
    start_year : int
        First year to download (YYYY, inclusive).
    end_year : int
        Last year to download (YYYY, inclusive).
    temporal : str, optional
        "daily" or "monthly" ,(default: "daily").
    PARAMS : Dict[str] : variables request from API
    Returns :
        Table with one row per date and columns:
        date
    """

    if PARAMS is None:
        raise ValueError("You must pass a list of variable names via PARAMS.")

    if temporal not in ("daily", "monthly"):
        raise ValueError("temporal must be 'daily' or 'monthly'")

    start = f"{start_year}0101"
    end = f"{end_year}1231"

    url = (
        f"https://power.larc.nasa.gov/api/temporal/{temporal}/point"
        f"?parameters={','.join(PARAMS)}"
        f"&community=AG&longitude={lon}&latitude={lat}"
        f"&start={start}&end={end}&format=JSON"
    )

    r = requests.get(url, timeout=90)
    if not r.ok or "application/json" not in r.headers.get("Content-Type", "").lower():
        raise RuntimeError(f"POWER error {r.status_code}: {r.text[:200]}")

    param = r.json().get("properties", {}).get("parameter", {})

    rows = []
    for var, series in param.items():
        for key, val in series.items():
            if temporal == "daily":
                y, m, d = int(key[:4]), int(key[4:6]), int(key[6:8])
            else:
                y, m, d = int(key[:4]), int(key[4:]), 1

            rows.append(
                {"year": y, "month": m, "day": d, "var": var, "value": float(val)}
            )

    long = pd.DataFrame(rows).sort_values(["year", "month", "day"])
    wide = (
        long.pivot(index=["year", "month", "day"], columns="var", values="value")
        .reset_index()
    )

    wide["date"] = pd.to_datetime(dict(year=wide.year, month=wide.month, day=wide.day))

    cols = ["date", "year", "month", "day"] + [
        c for c in wide.columns if c not in ("date", "year", "month", "day")
    ]
    return wide[cols]


def fetch_region_weather_mean(
    region_name, start_year, end_year, temporal="daily", REGION_POINTS=None, PARAMS=None
):
    """
    Fetch NASA POWER weather for a region and average over its subpoints.
    """
    if REGION_POINTS is None:
        raise ValueError("You must pass REGION_POINTS (dict of regions -> points).")

    if PARAMS is None:
        raise ValueError("You must pass PARAMS (list of NASA variables).")

    if region_name not in REGION_POINTS:
        raise ValueError(f"Region '{region_name}' not in REGION_POINTS")

    dfs = []
    for label, lat, lon in REGION_POINTS[region_name]:
        df_pt = fetch_power(
            lat, lon, start_year, end_year, temporal=temporal, PARAMS=PARAMS
        )
        df_pt["subpoint"] = label
        df_pt["Regions"] = region_name
        dfs.append(df_pt)

    df_all = pd.concat(dfs, ignore_index=True)

    meta_cols = ["date", "year", "month", "day", "subpoint", "Regions"]
    weather_cols = [c for c in df_all.columns if c not in meta_cols]

    df_region = (
        df_all.groupby(["date", "year", "month", "day", "Regions"], as_index=False)[
            weather_cols
        ]
        .mean()
    )
    return df_region

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
import matplotlib

matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42


def get_data_solar():
    engine = create_engine("sqlite:///failed_tests.db")
    sql_wind = """
    SELECT 
        longitude,
        latitude,
        area_geometry_with_buffer       
    FROM solar
    WHERE failed_test = 'point_in_area__district'
    """
    df_wind = pd.read_sql(sql=sql_wind, con=engine)
    gdf = gpd.GeoDataFrame(
        df_wind,
        geometry=gpd.points_from_xy(df_wind.longitude, df_wind.latitude),
        crs="EPSG:4326",
    ).to_crs("EPSG:4839")
    gdf_districts = gpd.GeoSeries.from_wkb(
        df_wind["area_geometry_with_buffer"], crs="EPSG:4326"
    ).to_crs("EPSG:4839")
    return gdf, gdf_districts


def get_data_wind():
    engine = create_engine(f"sqlite:///failed_tests.db")
    sql_wind = """
    SELECT 
        longitude,
        latitude,
        area_geometry_with_buffer       
    FROM wind
    WHERE failed_test = 'point_in_area__district'
    """
    df_wind = pd.read_sql(sql=sql_wind, con=engine)
    gdf = gpd.GeoDataFrame(
        df_wind,
        geometry=gpd.points_from_xy(df_wind.longitude, df_wind.latitude),
        crs="EPSG:4326",
    ).to_crs("EPSG:4839")
    gdf_districts = gpd.GeoSeries.from_wkb(
        df_wind["area_geometry_with_buffer"], crs="EPSG:4326"
    ).to_crs("EPSG:4839")
    return gdf, gdf_districts


def generate_data():
    df_wind, districts_wind = get_data_wind()
    df_solar, districts_solar = get_data_solar()

    wind_distance = df_wind["geometry"].distance(districts_wind, align=False).to_numpy()
    solar_distance = (
        df_solar["geometry"].distance(districts_solar, align=False).to_numpy()
    )
    print(f"Lowest distance of wind turbines to district is {np.min(wind_distance)}m")
    print(f"Lowest distance of solar systems to district is {np.min(solar_distance)}m")

    wind_capped_distance_in_km = (
        np.fmin(
            wind_distance, np.array([SOLAR_CAPPED_DISTANCE * 1000] * len(wind_distance))
        )
        / 1000
    )
    solar_capped_distance_in_km = (
        np.fmin(
            solar_distance,
            np.array([SOLAR_CAPPED_DISTANCE * 1000] * len(solar_distance)),
        )
        / 1000
    )

    np.save("solar_capped_distance_in_km.npy", solar_capped_distance_in_km)
    np.save("wind_capped_distance_in_km.npy", wind_capped_distance_in_km)

    return wind_capped_distance_in_km, solar_capped_distance_in_km


GENERATE_DATA = False

if GENERATE_DATA:
    wind_capped_distance_in_km, solar_capped_distance_in_km = generate_data()
else:
    wind_capped_distance_in_km = np.load("wind_capped_distance_in_km.npy")
    solar_capped_distance_in_km = np.load("solar_capped_distance_in_km.npy")


fig, ax = plt.subplots(1, 1, figsize=(6, 3))
FONTSIZE = 13


colors = ["#003366", "#2575c4", "#c44e52", "#ff981a"]
cmap = LinearSegmentedColormap.from_list("my_colormap", colors)

SOLAR_CAPPED_DISTANCE = 300
ax.hist(
    wind_capped_distance_in_km,
    bins=SOLAR_CAPPED_DISTANCE,
    range=(0, SOLAR_CAPPED_DISTANCE),
    color=colors[0],
    label="wind",
    alpha=0.7,
)
ax.hist(
    solar_capped_distance_in_km,
    bins=SOLAR_CAPPED_DISTANCE,
    range=(0, SOLAR_CAPPED_DISTANCE),
    color=colors[2],
    label="solar",
    alpha=0.7,
)
ax.set_xlabel("Distance [km]", fontsize=FONTSIZE)
ax.set_ylabel("Number Incorrect Units", fontsize=FONTSIZE)
ax.tick_params(axis="both", labelsize=FONTSIZE)
ax.legend()


ax.set_xticks([0, 100, 200, 300])

plt.tight_layout()
plt.savefig("figure_appendix_distance.pdf", bbox_inches="tight")
plt.show()

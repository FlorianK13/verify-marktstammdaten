import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import os
import json
import numpy as np
from collections import Counter

import pdb


def get_data_histogram_decimals():
    engine = create_engine("sqlite:///failed_tests.db")

    techs = ["biomass", "combustion", "hydro", "solar", "storage", "wind"]
    area = "district"

    data = []
    num_decs = []

    for tech in techs:
        print(tech)
        df = pd.read_sql(
            sql=f"SELECT * FROM {tech} WHERE failed_test = 'point_in_area__{area}';",
            con=engine,
        )
        lon, lat = df["longitude"].astype(str), df["latitude"].astype(str)

        lon_after_denominator = [elem.split(".")[1][:2] for elem in lon]
        lat_after_denominator = [elem.split(".")[1][:2] for elem in lat]

        lon_num_digits = [len(elem.split(".")[1]) for elem in lon]
        lat_num_digits = [len(elem.split(".")[1]) for elem in lat]

        data.append(lon_after_denominator)
        data.append(lat_after_denominator)
        num_decs.append(lon_num_digits)
        num_decs.append(lat_num_digits)

    return data, num_decs


CREATE_DATA = True
if CREATE_DATA:
    data, num_decs = get_data_histogram_decimals()
    flatten_data = [item for sublist in data for item in sublist]
    int_data = [int(item) for item in flatten_data]
    flatten_num_decs = [item for sublist in num_decs for item in sublist]
    os.makedirs(os.path.dirname("tmp/hist_coordinates.json"), exist_ok=True)
    with open("tmp/hist_coordinates.json", "w") as f:
        json.dump(int_data, f)
    with open("tmp/num_decs.json", "w") as f:
        json.dump(flatten_num_decs, f)
else:
    with open("tmp/hist_coordinates.json", "r") as f:
        int_data = json.load(f)
    with open("tmp/num_decs.json", "r") as f:
        flatten_num_decs = json.load(f)
pdb.set_trace()
count = Counter(flatten_num_decs)
print("Count of Number of digits after the denominator.")
print(count)
print("Total number of wrong coordinates:", len(flatten_num_decs))
colors_line = ["#003366", "#2575c4", "#c44e52", "#ff981a", "#66cc66", "#993399"]
colors_line_sec = ["#004080", "#3089d6", "#d55a5f", "#ffab33", "#70d870", "#b344b3"]

fig, ax2 = plt.subplots(1, 1, figsize=(3, 3))
ax2.hist(
    int_data,
    bins=range(0, 101),
    color=colors_line[1],
    edgecolor=colors_line_sec[0],
    alpha=0.7,
)

ax2.set_xlabel("Decimal Portion of Coordinates")
ax2.set_ylabel("Frequency")
ax2.set_yscale("log")
ax2.set_xticks(ticks=range(0, 101, 10))
ax2.set_xticklabels([str(x) if x % 20 == 0 else "" for x in range(0, 101, 10)])


plt.tight_layout()
plt.savefig("tmp/hist_coords.pdf")

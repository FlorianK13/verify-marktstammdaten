import matplotlib.pyplot as plt
import matplotlib
import matplotlib.patches as mpatches
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
import os
import json


def get_data():
    engine = create_engine("sqlite:///failed_tests.db")
    sql_inverter = """
    SELECT 
        power_gross as "Power of installed Modules in kW",
        power_inverter as "Power Inverter in kW",
        power_net as "Net Power in kW"
    FROM solar
    WHERE failed_test = 'column_division__power_gross_power_inverter_10'
    """
    df_inverter = pd.read_sql(sql=sql_inverter, con=engine)
    sql_area = """
    SELECT 
        power_gross AS "Power of installed PV system in kW",
        utilized_area AS "Area of the ground mounted PV system in ha",
        power_gross / utilized_area AS "Power per area in kW/ha"             
    FROM solar
    WHERE failed_test = 'column_division__power_gross_utilized_area'
    """
    df_area = pd.read_sql(sql=sql_area, con=engine)
    sql_modules = """
    SELECT 
        power_gross as "Power of installed Modules in kW",
        number_of_modules as "Number of modules",
        ROUND(power_gross / number_of_modules, 2) as "Power per module in kW"
    FROM solar
    WHERE failed_test = 'column_division__power_gross_number_modules'
    """
    df_modules = pd.read_sql(sql=sql_modules, con=engine)

    sql_solar_locations = """
    UNION ALL
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__solar') * 100, 3) as share,
        "Solar" as category
    FROM solar
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 0
    UNION ALL
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__solar') * 100, 3) as share,
        "Solar, DSO approved" as category
    FROM solar
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 1
    """

    sql_wind_locations = """
    UNION ALL
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__wind') * 100, 3) as share,
        "Wind" as category
    FROM wind
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 0
    UNION ALL
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__wind') * 100, 3) as share,
        "Wind, DSO approved" as category
    FROM wind
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 1
    """

    sql_biomass_locations = """
    UNION ALL
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__biomass') * 100, 3) as share,
        "Biomass" as category
    FROM biomass
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 0
    UNION ALL
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__biomass') * 100, 3) as share,
        "Biomass, DSO approved" as category
    FROM biomass
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 1
    """

    sql_hydro_locations = """
    UNION ALL
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__hydro') * 100, 3) as share,
        "Hydro" as category
    FROM hydro
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 0
    UNION ALL
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__hydro') * 100, 3) as share,
        "Hydro, DSO approved" as category
    FROM hydro
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 1
    """

    sql_combustion_locations = """
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__combustion') * 100, 3) as share,
        "Combustion" as category
    FROM combustion
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 0
    UNION ALL
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__combustion') * 100, 3) as share,
        "Combustion, DSO approved" as category
    FROM combustion
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 1
    """

    sql_storages_locations = """
    UNION ALL
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__storages') * 100, 3) as share,
        "Storages" as category
    FROM storage
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 0
    UNION ALL
    SELECT 
        ROUND(CAST(COUNT(DISTINCT(mastr_id)) AS FLOAT) / (SELECT number_rows_with_coordinates FROM metadata WHERE table_name='stg_mastr__storages') * 100, 3) as share,
        "Storages, DSO approved" as category
    FROM storage
    WHERE failed_test = 'point_in_area__district' AND operating_status = 'In Betrieb' AND grid_operator_inspection = 1
    """

    sql_locations = (
        sql_combustion_locations
        + sql_storages_locations
        + sql_biomass_locations
        + sql_solar_locations
        + sql_hydro_locations
        + sql_wind_locations
    )
    df_locations = pd.read_sql(sql=sql_locations, con=engine)

    return df_inverter, df_modules, df_area, df_locations


def divisions_line_plot():
    def get_data_storage(divisions):
        engine = create_engine("sqlite:///failed_tests.db")

        percentages = {}

        metadata = pd.read_sql(
            sql="SELECT * FROM metadata WHERE table_name = 'stg_mastr__storages';",
            con=engine,
        )

        for div in divisions:
            query = f"SELECT * FROM storage WHERE failed_test = 'column_division__power_gross_power_inverter_{div}';"
            print(query)
            df = pd.read_sql(sql=query, con=engine)
            dso_yes = df[(df["grid_operator_inspection"] == "1")]
            dso_no = df[(df["grid_operator_inspection"] == "0")]
            percentages[f"div-{div}"] = {
                "DSO yes": len(dso_yes) / metadata["number_rows_dso_approved"][0],
                "DSO no": len(dso_no) / metadata["number_rows_dso_not_approved"][0],
            }

        return percentages

    def get_data_solar(divisions):
        engine = create_engine("sqlite:///failed_tests.db")

        percentages = {}

        metadata = pd.read_sql(
            sql="SELECT * FROM metadata WHERE table_name = 'stg_mastr__solar';",
            con=engine,
        )

        for div in divisions:
            query = f"SELECT * FROM solar WHERE failed_test = 'column_division__power_gross_power_inverter_{div}';"
            print(query)
            df = pd.read_sql(sql=query, con=engine)
            dso_yes_larger_30 = df[
                (df["grid_operator_inspection"] == "1") & (df["power_gross"] > 30)
            ]
            dso_yes_smaller_30 = df[
                (df["grid_operator_inspection"] == "1") & (df["power_gross"] < 30)
            ]
            dso_no_larger_30 = df[
                (df["grid_operator_inspection"] == "0") & (df["power_gross"] > 30)
            ]
            dso_no_smaller_30 = df[
                (df["grid_operator_inspection"] == "0") & (df["power_gross"] < 30)
            ]
            percentages[f"div-{div}"] = {
                "DSO yes": {
                    "larger 30": len(dso_yes_larger_30)
                    / metadata["number_rows_with_coordinates_dso_approved"][0],
                    "smaller 30": len(dso_yes_smaller_30)
                    / metadata["number_rows_no_coordinates_dso_approved"][0],
                },
                "DSO no": {
                    "larger 30": len(dso_no_larger_30)
                    / (
                        metadata["number_rows_with_coordinates"][0]
                        - metadata["number_rows_with_coordinates_dso_approved"][0]
                    ),
                    "smaller 30": len(dso_no_smaller_30)
                    / (
                        metadata["number_rows"][0]
                        - metadata["number_rows_with_coordinates"][0]
                        - metadata["number_rows_no_coordinates_dso_approved"][0]
                    ),
                },
            }

        return percentages

    CREATE_DATA_STORAGE = False
    CREATE_DATA_SOLAR = False
    filename_storage = "tmp/figure_percentages_storage.json"
    filename_solar = "tmp/figure_percentages_solar.json"

    divisions = [3, 5, 8, 10, 15, 20]

    if CREATE_DATA_SOLAR:
        data_solar = get_data_solar(divisions)
        os.makedirs(os.path.dirname(filename_solar), exist_ok=True)
        with open(filename_solar, "w") as f:
            json.dump(data_solar, f)
    else:
        with open(filename_solar, "r") as f:
            data_solar = json.load(f)

    if CREATE_DATA_STORAGE:
        data_storages = get_data_storage(divisions)
        os.makedirs(os.path.dirname(filename_storage), exist_ok=True)
        with open(filename_storage, "w") as f:
            json.dump(data_storages, f)
    else:
        with open(filename_storage, "r") as f:
            data_storages = json.load(f)

    solar_dso_yes_small = []
    solar_dso_no_small = []
    solar_dso_yes_large = []
    solar_dso_no_large = []

    for key, value in data_solar.items():
        print(key)
        solar_dso_yes_small.append(value["DSO yes"]["smaller 30"])
        solar_dso_no_small.append(value["DSO no"]["smaller 30"])
        solar_dso_yes_large.append(value["DSO yes"]["larger 30"])
        solar_dso_no_large.append(value["DSO no"]["larger 30"])

    storage_dso_yes, storage_dso_no = [], []
    for key, value in data_storages.items():
        storage_dso_yes.append(value["DSO yes"])
        storage_dso_no.append(value["DSO no"])

    return (
        divisions,
        solar_dso_yes_small,
        solar_dso_no_small,
        solar_dso_yes_large,
        solar_dso_no_large,
        storage_dso_yes,
        storage_dso_no,
    )


df_inverter, df_modules, df_area, df_locations = get_data()
FIG1 = True
FIG2 = False
FONTSIZE = 13
(
    divisions,
    solar_dso_yes_small,
    solar_dso_no_small,
    solar_dso_yes_large,
    solar_dso_no_large,
    storage_dso_yes,
    storage_dso_no,
) = divisions_line_plot()

colors = ["#003366", "#2575c4", "#c44e52", "#ff981a"]
colors_line = ["#003366", "#2575c4", "#c44e52", "#ff981a", "#66cc66", "#993399"]
colors_line_sec = ["#004080", "#3089d6", "#d55a5f", "#ffab33", "#70d870", "#b344b3"]
cmap = LinearSegmentedColormap.from_list("my_colormap", colors)

if FIG1:
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(1, 4, figsize=(12, 3), sharey=False)
    plt.subplots_adjust(wspace=0.5)
    #################
    ## Constants ####
    #################

    ALPHA = 0.5
    CMAP = cmap

    #################
    #   ax1         #
    #################
    ratio_ax1 = np.log(
        df_inverter["Power of installed Modules in kW"]
        / df_inverter["Power Inverter in kW"]
    )
    ax1_scatter = ax1.scatter(
        df_inverter["Power Inverter in kW"],
        df_inverter["Power of installed Modules in kW"],
        c=ratio_ax1,
        alpha=ALPHA,
        s=0.2,
        cmap=CMAP,
        norm=plt.Normalize(vmin=ratio_ax1.min(), vmax=ratio_ax1.max()),
        rasterized=True,  # this turns vectors to pixels in the final pdf
    )

    ax1.set_xlabel("Power Inverter [kW]")
    ax1.set_ylabel("Power PV system [kW]")
    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax1.grid(True, which="both", ls="--", lw=0.5, color="gray", alpha=0.4)
    ax1.text(
        0.05,
        0.95,
        "a)",
        transform=ax1.transAxes,
        fontsize=FONTSIZE,
        verticalalignment="top",
    )
    cbar = plt.colorbar(ax1_scatter, ax=ax1)
    cbar.set_label(
        r"$\log\ (P_{\text{PV}} / P_{\text{I}})$",
        fontsize=8,
        labelpad=5,
        rotation=0,
        ha="right",
        position=(0, 1.1),
    )

    #################
    #   ax2         #
    #################
    ratio_ax2 = np.log(
        df_modules["Power of installed Modules in kW"] / df_modules["Number of modules"]
    )
    ax2_scatter = ax2.scatter(
        df_modules["Number of modules"],
        df_modules["Power of installed Modules in kW"],
        c=ratio_ax2,
        alpha=ALPHA,
        s=0.2,
        cmap=CMAP,
        norm=plt.Normalize(vmin=ratio_ax2.min(), vmax=ratio_ax2.max()),
        rasterized=True,  # this turns vectors to pixels in the final pdf
    )

    ax2.set_xlabel("Number modules")

    ax2.set_xscale("log")
    ax2.set_yscale("log")
    ax2.grid(True, which="both", ls="--", lw=0.5, color="gray", alpha=0.4)
    ax2.text(
        0.05,
        0.95,
        "b)",
        transform=ax2.transAxes,
        fontsize=FONTSIZE,
        verticalalignment="top",
    )
    cbar = plt.colorbar(ax2_scatter, ax=ax2)
    cbar.set_label(
        r"$\log\ (P_{\text{PV}} / N_{\text{modules}})$",
        fontsize=8,
        labelpad=5,
        rotation=0,
        ha="right",
        position=(0, 1.1),
    )

    #################
    #   ax3         #
    #################
    ratio_ax3 = np.log(
        df_area["Power of installed PV system in kW"]
        / df_area["Area of the ground mounted PV system in ha"]
    )
    ax3_scatter = ax3.scatter(
        df_area["Area of the ground mounted PV system in ha"],
        df_area["Power of installed PV system in kW"],
        c=ratio_ax3,
        alpha=ALPHA,
        s=0.2,
        cmap=CMAP,
        norm=plt.Normalize(vmin=ratio_ax3.min(), vmax=ratio_ax3.max()),
        rasterized=True,  # this turns vectors to pixels in the final pdf
    )

    ax3.set_xlabel("Area [ha]")
    ax3.set_xscale("log")
    ax3.set_yscale("log")
    ax3.set_xlim(0.00001, 100)
    ax3.grid(True, which="both", ls="--", lw=0.5, color="gray", alpha=0.4)
    ax3.text(
        0.05,
        0.95,
        "c)",
        transform=ax3.transAxes,
        fontsize=FONTSIZE,
        verticalalignment="top",
    )
    cbar = plt.colorbar(ax3_scatter, ax=ax3)
    cbar.set_label(
        r"$\log (P_{\text{PV}} / A)$",
        fontsize=8,
        labelpad=5,
        rotation=0,
        ha="right",
        position=(0, 1.1),
    )

    #################
    #   ax4         #
    #################

    sty = ["o-", "^-", "v-", "<-", "*-", ">-", "o-", "^-", "p-", "h-", "3-"]

    ax4.plot(
        divisions,
        np.array(storage_dso_yes) * 100,
        sty[0],
        label=r"storage, DSO $\checkmark$",
        color=colors_line[0],
        mec=colors_line[0],
        mfc=colors_line_sec[0],
    )
    ax4.plot(
        divisions,
        np.array(storage_dso_no) * 100,
        sty[1],
        label="storage, DSO x",
        color=colors_line[1],
        mec=colors_line[1],
        mfc=colors_line_sec[1],
    )
    ax4.plot(
        divisions,
        np.array(solar_dso_yes_small) * 100,
        sty[2],
        label=r"solar<30 DSO $\checkmark$",
        color=colors_line[2],
        mec=colors_line[2],
        mfc=colors_line_sec[2],
    )
    ax4.plot(
        divisions,
        np.array(solar_dso_no_small) * 100,
        sty[3],
        label="solar<30 DSO x ",
        color=colors_line[3],
        mec=colors_line[3],
        mfc=colors_line_sec[3],
    )
    ax4.plot(
        divisions,
        np.array(solar_dso_yes_large) * 100,
        sty[4],
        label=r"solar>30 DSO $\checkmark$",
        color=colors_line[4],
        mec=colors_line[4],
        mfc=colors_line_sec[4],
    )
    ax4.plot(
        divisions,
        np.array(solar_dso_no_large) * 100,
        sty[5],
        label="solar>30 DSO x",
        color=colors_line[5],
        mec=colors_line[5],
        mfc=colors_line_sec[5],
    )

    ax4.set_xlabel(r"$P_{\text{PV}} / P_{\text{I}}$")
    ax4.set_ylabel("Share of Failed Tests [%]")
    ax4.grid(True, which="both", ls="--", lw=0.5, color="gray", alpha=0.4)
    ax4.text(
        0.05,
        0.95,
        "d)",
        transform=ax4.transAxes,
        fontsize=FONTSIZE,
        verticalalignment="top",
    )
    ax4.set_xticks([3, 5, 10, 15, 20])
    ax4.set_ylim(-0.2, 11)
    ax4.legend(loc="upper right", handlelength=1)

    plt.tight_layout()

    plt.savefig("figure_results_scatter.pdf", dpi=500, bbox_inches="tight")

if FIG2:
    #################
    #   ax4         #
    #################
    fig, ax4 = plt.subplots(1, 1, figsize=(3.7, 3), sharey=True)
    bar_colors = [colors[0], colors[1]] * (len(df_locations) // 2)
    hatches = ["", ""] * (len(df_locations) // 2)
    y_positions = np.arange(len(df_locations))
    y_positions_labels = np.arange(len(df_locations))[::2] + 0.5
    bar_width = 0.8

    for i, (pos, row) in enumerate(df_locations.iterrows()):
        ax4.barh(
            y_positions[i],
            row["share"],
            color=bar_colors[i],
            height=bar_width,
            label=row["category"],
            hatch=hatches[i],
            alpha=0.7,
        )
    ax4.set_yticks(y_positions_labels)
    ax4.set_yticklabels(df_locations["category"][::2])
    ax4.text(
        0.2,
        0.95,
        "d)",
        transform=ax4.transAxes,
        fontsize=FONTSIZE,
        verticalalignment="top",
    )

    ax4.set_xlabel("Share [in %]")
    ax4.invert_yaxis()

    legend_handles = [
        mpatches.Patch(color=bar_colors[0], label="DSO x", hatch=hatches[0]),
        mpatches.Patch(
            color=bar_colors[1], label=r"DSO $\checkmark$", hatch=hatches[1]
        ),
    ]
    ax4.legend(handles=legend_handles, loc="upper right", handlelength=1)

    plt.tight_layout()
    plt.savefig("figure_results_locations.pdf", bbox_inches="tight")

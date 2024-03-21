import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine
import numpy as np


def get_data():
    engine = create_engine(f"sqlite:///failed_tests.db")
    sql_inverter = """
    SELECT 
        power_gross as "Power of installed Modules in kW",
        power_inverter as "Power Inverter in kW",
        power_net as "Net Power in kW"
    FROM solar
    WHERE failed_test = 'column_division__power_gross_power_inverter'
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

    return df_inverter, df_modules, df_area


df_inverter, df_modules, df_area = get_data()

fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(10, 3), sharey=True)
plt.subplots_adjust(wspace=0.5)
#################
## Constants ####
#################

ALPHA = 0.5
CMAP = "plasma"

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
    0.05, 0.95, "a)", transform=ax1.transAxes, fontsize=12, verticalalignment="top"
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
    0.05, 0.95, "b)", transform=ax2.transAxes, fontsize=12, verticalalignment="top"
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
    0.05, 0.95, "c)", transform=ax3.transAxes, fontsize=12, verticalalignment="top"
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
plt.tight_layout()

plt.savefig("figure_results_scatter.pdf", dpi=500, bbox_inches="tight")
plt.show()

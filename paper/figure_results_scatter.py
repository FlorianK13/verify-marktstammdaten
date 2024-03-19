import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine


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

#################
#   ax1         #
#################

ax1_scatter = ax1.scatter(
    df_inverter["Power Inverter in kW"],
    df_inverter["Power of installed Modules in kW"],
    alpha=0.3,
    s=0.1,
)

ax1.set_xlabel("Power Inverter [kW]")
ax1.set_ylabel("Power PV system [kW]")
ax1.set_xscale("log")
ax1.set_yscale("log")
ax1.grid(True, which="both", ls="--", lw=0.5, color="gray", alpha=0.4)
ax1.text(
    0.05, 0.95, "a)", transform=ax1.transAxes, fontsize=12, verticalalignment="top"
)


#################
#   ax2         #
#################

ax2_scatter = ax2.scatter(
    df_modules["Number of modules"],
    df_modules["Power of installed Modules in kW"],
    alpha=0.3,
    s=0.1,
)

ax2.set_xlabel("Number modules")
# ax2.set_ylabel("Power [kW]")
ax2.set_xscale("log")
ax2.set_yscale("log")
ax2.grid(True, which="both", ls="--", lw=0.5, color="gray", alpha=0.4)
ax2.text(
    0.05, 0.95, "b)", transform=ax2.transAxes, fontsize=12, verticalalignment="top"
)

#################
#   ax3         #
#################

ax3_scatter = ax3.scatter(
    df_area["Area of the ground mounted PV system in ha"],
    df_area["Power of installed PV system in kW"],
    alpha=0.3,
    s=0.1,
)

ax3.set_xlabel("Area [ha]")
# ax3.set_ylabel("Power [kW]")
ax3.set_xscale("log")
ax3.set_yscale("log")
ax3.grid(True, which="both", ls="--", lw=0.5, color="gray", alpha=0.4)
ax3.text(
    0.05, 0.95, "c)", transform=ax3.transAxes, fontsize=12, verticalalignment="top"
)

plt.show()

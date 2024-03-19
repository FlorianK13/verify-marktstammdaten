import os
import yaml
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import pdb


def get_engine():
    POSTGRES_DB = "verify-marktstammdatenregister"
    POSTGRES_USER = "postgres"
    POSTGRES_PASSWORD = "postgres"
    PORT = "5512"
    return create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{PORT}/{POSTGRES_DB}"
    )


def create_table_from_yaml(yaml_path, tables):
    engine = get_engine()
    names, completeness, examples = [], [], []
    with open(yaml_path, "r") as file:
        yaml_content = yaml.safe_load(file)
    model_name = yaml_content["models"][0]["name"]
    df = pd.read_sql(f"SELECT * FROM dbt.{model_name}", con=engine)
    for column in yaml_content["models"][0]["columns"]:
        names.append(column["name"].replace("_", " "))
        example, completeness_score = get_column_statistics(df, column["name"])
        examples.append(example)
        completeness.append(completeness_score)
    tables[model_name.split("__")[1]] = {
        "column": names,
        "example": examples,
        "completeness": completeness,
    }
    return tables


def get_column_names(nested_dict):
    column_names = []
    for _, inner_dict in nested_dict.items():
        if "column" in inner_dict:
            column_names.extend(inner_dict["column"])
    return list(set(column_names))


def get_column_statistics(df, column):
    completeness_score = int(
        np.round(df[column].notnull().sum() / len(df) * 100, decimals=0)
    )
    example = df[column].mode()[0] if column != "coordinate" else "48.1748, 11.5961"
    return example, completeness_score


yaml_folder = "dbt\\models"
tables = {}

for filename in os.listdir(yaml_folder):
    if filename.endswith(".yml") and filename != "sources.yml":
        print(filename)
        tables = create_table_from_yaml(os.path.join(yaml_folder, filename), tables)


all_columns = get_column_names(tables)

df = pd.DataFrame(
    data={
        "column": all_columns,
        "example": [""] * len(all_columns),
        "$C_{biomass}$": ["x"] * len(all_columns),
        "$C_{combustion}$": ["x"] * len(all_columns),
        "$C_{hydro}$": ["x"] * len(all_columns),
        "$C_{solar}$": ["x"] * len(all_columns),
        "$C_{storages}$": ["x"] * len(all_columns),
        "$C_{wind}$": ["x"] * len(all_columns),
    },
)
df.set_index("column", inplace=True)

for model, details in tables.items():
    df_model = pd.DataFrame(details)
    for _, row in df_model.iterrows():

        df.at[row["column"], "example"] = row["example"]
        df.at[row["column"], "$C_{" + model + "}$"] = int(row["completeness"])
pdb.set_trace()
df.to_latex(f"paper\\appendix\\all_techs.tex")

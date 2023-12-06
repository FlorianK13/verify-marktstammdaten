import os

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, inspect

from load_raw_data import get_engine


def main():
    postgres_engine = get_engine()
    sqlite_database_name = "failed_tests.db"
    sqlite_engine = create_engine(f"sqlite:///{sqlite_database_name}")
    if os.path.exists(sqlite_database_name):
        os.remove(sqlite_database_name)
    failed_tests_schema = "public_dbt_test__audit"
    inspector = inspect(postgres_engine)
    test_tables = inspector.get_table_names(schema=failed_tests_schema)

    for table in test_tables:
        query = f'SELECT * FROM "{failed_tests_schema}"."{table}"'
        df = pd.read_sql(query, con=postgres_engine)
        if len(df) == 0:
            continue
        test_name = get_test_from_table_name(table)
        technology = get_technology_from_table_name(table)
        df["failed_test"] = test_name
        df.drop(columns=["coordinate"], inplace=True)
        try:
            df.to_sql(name=technology, con=sqlite_engine, if_exists="append")
        except sqlalchemy.exc.OperationalError:
            df = delete_columns_from_df_that_arenot_in_database(
                df=df, sqlite_engine=sqlite_engine, table=technology
            )
            df.to_sql(name=technology, con=sqlite_engine, if_exists="append")


def get_test_from_table_name(table_name: str):
    first_underscore_index = table_name.index("_")
    return table_name[first_underscore_index + 1 :]


def get_technology_from_table_name(table_name: str):
    first_underscore_index = table_name.index("_")
    return table_name[0:first_underscore_index]


def delete_columns_from_df_that_arenot_in_database(df, sqlite_engine, table):
    inspector = sqlalchemy.inspect(sqlite_engine)
    columns_database = [item["name"] for item in inspector.get_columns(table)]
    columns_df = df.columns
    columns_to_delete = [item for item in columns_df if item not in columns_database]
    print(
        f"The following columns will not be written to the database: {columns_to_delete}"
    )
    df.drop(columns=columns_to_delete, inplace=True)
    return df


if __name__ == "__main__":
    main()

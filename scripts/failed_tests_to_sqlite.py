import os

import pandas as pd
import sqlalchemy
from load_raw_data import get_engine
from sqlalchemy import create_engine, inspect, text


def main():
    postgres_engine = get_engine()
    sqlite_database_name = "failed_tests.db"
    sqlite_engine = create_engine(f"sqlite:///{sqlite_database_name}")
    if os.path.exists(sqlite_database_name):
        os.remove(sqlite_database_name)
    failed_tests_schema = "dbt_dbt_test__audit"
    inspector = inspect(postgres_engine)
    test_tables = inspector.get_table_names(schema=failed_tests_schema)

    for table in test_tables:
        query = f'SELECT * FROM "{failed_tests_schema}"."{table}"'
        df = pd.read_sql(query, con=postgres_engine)
        if len(df) == 0:
            continue
        add_failed_tests_to_sqlite(df, table, engine=sqlite_engine)
    create_mastr_metadata_table(
        input_engine=postgres_engine, output_engine=sqlite_engine
    )
    describe_final_database(engine=sqlite_engine)


def get_test_from_table_name(table_name: str):
    first_underscore_index = table_name.index("_")
    return table_name[first_underscore_index + 1 :]


def add_failed_tests_to_sqlite(df, table, engine):
    test_name = get_test_from_table_name(table)
    technology = get_technology_from_table_name(table)
    df["failed_test"] = test_name
    df.drop(columns=["coordinate"], inplace=True)
    try:
        df.to_sql(name=technology, con=engine, if_exists="append")
    except sqlalchemy.exc.OperationalError:
        df = delete_columns_from_df_that_arenot_in_database(
            df=df, sqlite_engine=engine, table=technology
        )
        df.to_sql(name=technology, con=engine, if_exists="append")


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


def create_mastr_metadata_table(input_engine, output_engine):
    tables = [
        "stg_mastr__solar",
        "stg_mastr__biomass",
        "stg_mastr__wind",
        "stg_mastr__hydro",
        "stg_mastr__storages",
        "stg_mastr__combustion",
    ]
    metadata_df = pd.DataFrame(
        columns=["table_name", "number_rows", "number_rows_with_coordinates"]
    )
    for table in tables:
        if table == "stg_mastr__wind":
            metadata_df = add_metadata_to_dataframe(
                table=table,
                engine=input_engine,
                metadata_df=metadata_df,
                where_condition="position = 'Windkraft an Land'",
                table_name_replacement="wind_onshore",
            )
            metadata_df = add_metadata_to_dataframe(
                table=table,
                engine=input_engine,
                metadata_df=metadata_df,
                where_condition="position = 'Windkraft auf See'",
                table_name_replacement="wind_offshore",
            )

        metadata_df = add_metadata_to_dataframe(
            table=table, engine=input_engine, metadata_df=metadata_df
        )

    # Ad own metadata for ground mounted PV
    metadata_df = add_metadata_to_dataframe(
        table="stg_mastr__solar",
        engine=input_engine,
        metadata_df=metadata_df,
        where_condition="unit_type = 'Freifläche'",
        table_name_replacement="solar_ground_mounted",
    )

    metadata_df.to_sql("metadata", con=output_engine, if_exists="replace", index=False)


def add_metadata_to_dataframe(
    table, engine, metadata_df, where_condition=None, table_name_replacement=None
):
    query = (
        text(f'SELECT COUNT(*) FROM "dbt"."{table}"')
        if not where_condition
        else text(f'SELECT COUNT(*) FROM "dbt"."{table}" WHERE {where_condition}')
    )
    number_rows = engine.connect().execute(query).fetchone()
    query = (
        text(f'SELECT COUNT(*) FROM "dbt"."{table}" WHERE coordinate IS NOT NULL')
        if not where_condition
        else text(
            f'SELECT COUNT(*) FROM "dbt"."{table}" WHERE coordinate IS NOT NULL AND {where_condition}'
        )
    )
    number_rows_with_coordinates = engine.connect().execute(query).fetchone()
    table_name = table if not table_name_replacement else table_name_replacement
    metadata_df = pd.concat(
        [
            metadata_df,
            pd.DataFrame(
                {
                    "table_name": [table_name],
                    "number_rows": [number_rows[0]],
                    "number_rows_with_coordinates": number_rows_with_coordinates[0],
                }
            ),
        ],
    )
    return metadata_df


def describe_final_database(engine: sqlalchemy.Engine):

    connection = engine.connect()
    inspector = inspect(engine)
    for table in inspector.get_table_names():
        df = pd.read_sql(table, connection)
        print(f"Description of table: {table}")
        print(df.describe())
    connection.close()


if __name__ == "__main__":
    main()

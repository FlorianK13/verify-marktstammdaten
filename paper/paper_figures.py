import os
import pandas as pd
import yaml

folder_path = "../dbt/models"
test_records = []


def get_tests(test_records, data, simple_model_name):
    if not "tests" in data:
        return test_records
    for test in data["tests"]:
        for _, test_details in test.items():

            full_name = test_details.get("name", "")
            test_records.append(
                {
                    "Test_name": "_".join(full_name.split("_", 1)[1:]),
                    "technology": simple_model_name,
                    "test_details": test_details,
                }
            )
    return test_records


for filename in os.listdir(folder_path):
    if filename.endswith(".yml"):
        file_path = os.path.join(folder_path, filename)
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
            if not "models" in data:
                continue
            data = data["models"][0]
            model_name = data["name"]
            simple_model_name = model_name.split("__")[1]
            if "tests" in data:
                test_records = get_tests(test_records, data, simple_model_name)
            for column in data["columns"]:
                test_records = get_tests(test_records, column, simple_model_name)


df_tests = pd.DataFrame(
    test_records, columns=["Test_name", "technology", "test_details"]
)

print(df_tests)

df_tests["Value"] = "yes"
pivot_df = df_tests.pivot_table(
    index="Test_name",
    columns="technology",
    values="Value",
    aggfunc="first",
    fill_value="",
)
pivot_df.reset_index(inplace=True)
print(pivot_df)
pivot_df.to_latex("table.tex", index=False)

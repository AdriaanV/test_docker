from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from datetime import datetime, timedelta
from google.cloud import storage
import apache_beam as beam
import pandas as pd
import requests
import json
import time


headers = {
    "Content-Type": "application/json",
    "Authentication-Key": "VprbSGjZNAe15zRI40HdL8gkBM8qitF3",
    "Authentication-Secret": "JW4rseh6ElXALF9FvGI7FOh7rfnzzSt8",
}


def get_simplicate_data(endpoint: str, input_params: dict = {}) -> pd.DataFrame | str:

    base_url = f"https://multiply.simplicate.nl/api/v2/{endpoint}"
    params = {"metadata": "count", "limit": 100}
    params.update(input_params)

    response = requests.get(url=base_url, headers=headers, params=params)
    response_json = response.json()

    df = pd.DataFrame(response_json["data"], dtype=str)
    record_count = response_json["metadata"]["count"]

    if record_count > 100:
        for offset in range(100, record_count, 100):
            time.sleep(1)
            params["offset"] = offset
            response = requests.get(url=base_url, headers=headers, params=params)
            append_df = pd.DataFrame(response.json()["data"], dtype=str)
            df = pd.concat([df, append_df], ignore_index=True)

    return df


def _normalize_complex(df: pd.DataFrame, series_name: str) -> pd.DataFrame:
    series_normalized = pd.json_normalize(df[series_name])
    for col in series_normalized.columns:
        df[f"{series_name}_{col}"] = series_normalized[col].values
    return df


def _extract_and_map_custom_fields(
    df: pd.DataFrame, custom_field_map: dict
) -> pd.DataFrame:
    output_df = pd.json_normalize(df["custom_fields"])
    new_col_names = []

    for i, col in enumerate(output_df.columns):
        col_name = output_df[i][0]["label"]
        col_name_format = f"cf_{col_name.lower().replace(' ', '_')}"
        new_col_names.append(col_name_format)

    output_df.columns = new_col_names

    for col in output_df.columns:
        output_df[col] = output_df[col].apply(lambda x: x.get("value"))

    cf_columns = [col for col in output_df.columns if col.startswith("cf_")]

    for col in cf_columns:
        output_df[col] = output_df[col].map(custom_field_map)

    return output_df


def process_simplicate_dataset(
    input_df: pd.DataFrame,
    has_custom_fields: bool,
    to_normalize: list,
    custom_field_map: dict = {},
) -> pd.DataFrame:
    output_df = input_df

    for series_name in to_normalize:
        if series_name in output_df.columns:
            output_df = _normalize_complex(output_df, series_name)

    if has_custom_fields:
        df_expanded = _extract_and_map_custom_fields(output_df, custom_field_map)
        output_df = output_df.join(df_expanded).drop("custom_fields", axis=1)

    output_df.drop(to_normalize, axis=1, inplace=True)

    return output_df


def run_test():

    df_cust_fields = get_simplicate_data(endpoint="customfields/option")

    cust_fields_dict = pd.Series(
        df_cust_fields["label"].values, index=df_cust_fields["id"]
    ).to_dict()

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d %H:%M:%S")

    df_crm = get_simplicate_data(endpoint="crm/organization")

    to_process = [
        {
            "name": "CRM",
            "specifications": {
                "dataframe": df_crm,
                "has_custom_fields": True,
                "cols_to_normalize": [
                    "industry",
                    "visiting_address",
                    "organizationsize",
                    "relation_type",
                    "relation_manager",
                    "debtor",
                ],
                "cols_to_select": [],
                "cols_to_bq_datetime": [],
                "bq_schema": {
                    "fields": [
                        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "created", "type": "DATETIME", "mode": "NULLABLE"},
                        {"name": "boolean", "type": "BOOLEAN", "mode": "NULLABLE"},
                    ]
                },
                "bq_table_name": "stg.SIMPL_SAMPLE",
                "bq_write_disposition": BigQueryDisposition.WRITE_TRUNCATE,
                "bq_create_disposition": BigQueryDisposition.CREATE_IF_NEEDED,
            },
        },
    ]

    for entity in to_process:
        specs = entity["specifications"]
        dfp = specs["dataframe"]
        dfp = process_simplicate_dataset(
            input_df=dfp,
            has_custom_fields=specs["has_custom_fields"],
            to_normalize=specs["cols_to_normalize"],
            custom_field_map=cust_fields_dict,
        )
        # specs["dataframe"] = dfp
        if entity["name"] == "CRM":
            df_crm = dfp

    df_2 = df_crm[["name", "phone", "created"]].reset_index(drop=True)

    def simp_datetime_to_bq_datetime(input: str):
        return input.replace(" ", "T")

    convert_datetime_cols = ["created"]

    for col in convert_datetime_cols:
        df_2[col] = df_2[col].apply(lambda x: simp_datetime_to_bq_datetime(x))

    json_output = df_2.to_json(orient="records")

    # Your in-memory JSON data
    # json_data = [
    #     {"name": "John Doe", "age": 30},
    #     {"name": "Jane Doe", "age": 25},
    #     # Add more dictionaries representing your rows here
    # ]
    bucket_name = "bck_multidata_01"
    staging_folder = "temp"
    # Define your pipeline options
    pipeline_options = {
        "project": "multidata-388913",
        "runner": "DirectRunner",  # Change this to DataflowRunner to run on Google Cloud Dataflow
        # "direct_num_workers": 0,
        # 'region': 'YOUR_REGION',
        "temp_location": f"gs://{bucket_name}/{staging_folder}",
    }

    options = PipelineOptions(flags=[], **pipeline_options)

    # Define the schema of your BigQuery table
    table_schema = {
        "fields": [
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "created", "type": "DATETIME", "mode": "NULLABLE"},
            # {"name": "boolean", "type": "BOOLEAN", "mode": "NULLABLE"},
            # Add more fields here based on your JSON structure
        ]
    }

    data_to_load = json.loads(json_output)

    data_to_load = [
        {
            "name": "Selles Automatisering B.V.",
            "phone": "0383857579",
            "created": "2017-10-16T13:16:30",
        },
        {
            "name": "Schuurman Schoenen",
            "phone": "0545280080",
            "created": "2017-10-16T13:16:30",
        },
    ]

    def delete_folder(bucket_name, folder_name):
        """Deletes a folder in the specified Google Cloud Storage bucket.

        Args:
            bucket_name (str): The name of the bucket.
            folder_name (str): The name of the folder to delete.
        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=folder_name)

        for blob in blobs:
            blob.delete()
            # print(f"{blob.name} deleted.")

    try:
        with beam.Pipeline(options=options) as p:
            (
                p
                # Create PCollection from in-memory data
                | "CreatePCollection" >> beam.Create(data_to_load)
                # | 'MapToDict' >> beam.Map(lambda x: map_to_dict(x))
                # Write the records directly to BigQuery
                | "WriteToBigQuery"
                >> WriteToBigQuery(
                    "test.SIMPL_SAMPLE",
                    schema=table_schema,
                    # method="STREAMING_INSERTS",
                    method="FILE_LOADS",
                    # method="STORAGE_WRITE_API",
                    # triggering_frequency=5,
                    write_disposition=BigQueryDisposition.WRITE_TRUNCATE,  # https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.Write.WriteDisposition.html#WRITE_TRUNCATE
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                )
            )
    finally:
        delete_folder(bucket_name, staging_folder)


if __name__ == "__main__":
    run_test()

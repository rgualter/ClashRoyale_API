
import json
import logging
import pandas as pd
import os
import glob
import datetime
from abc import abstractmethod
from pathlib import Path

import boto3
import json
import re

s3 = boto3.client("s3")

def read_json_from_s3(bucket_name, file_name):
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    json_file = response["Body"].read().decode("utf-8").splitlines()
    return [json.loads(line) for line in json_file]

def read_multiple_jsons_from_s3(bucket_name, prefix, date_pattern):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    all_files = response.get("Contents", [])
    json_files = [f for f in all_files if re.match(date_pattern, f["Key"])]
    return [read_json_from_s3(bucket_name, f["Key"]) for f in json_files]

# exemplo de uso
bucket_name = "apiroyale-raw"

file_name = "APIRoyale/players/sub_type=battlelog/extracted_at=2023-02-05/#YLY8GJ0LY_2023-02-05 17:10:56.950056.json"
prefix = "my-json-folder/"
date_pattern = r".*\d{4}-\d{2}-\d{2}.*\.json"
json_data = read_json_from_s3(bucket_name, file_name)
#json_data = read_json_from_s3(bucket_name, prefix, date_pattern)
print(json_data)

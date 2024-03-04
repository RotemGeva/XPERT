import logging
import time
from datetime import datetime
from Backup import Backup
from Requirements import Requirements
import argparse
import pandas as pd
import os
import numpy as np
import json5 as json
from DefaultValue import DefaultValue

# Log settings
script_dir = os.path.dirname(os.path.realpath(__file__))
log_path = os.path.join(script_dir, 'Logs')
if not os.path.exists(log_path):
    os.makedirs(log_path)
logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.join(log_path, f"log_{datetime.now().strftime('%m-%d_%H-%M')}.log"), filemode='w',
                    format="%(asctime)s - %(levelname)s - %("
                           "message)s")


# Get all skipped files from a given path of an external file
def get_files_to_skip(path: str):
    try:
        with open(path, 'r') as file:
            json_data = json.load(file)
            return json_data
    except Exception:
        logging.error(f"Failed to read JSON file with skipped keys from {path}")
        raise Exception("Failed to read JSON file with skipped keys")


def check_versions_exist(versions: list):  # Verify given versions exist in dest
    dest = "D:\\FusWs"
    curr_versions = os.listdir(dest)
    for version in versions:
        curr_version = [s for s in curr_versions if version in s]
        if not curr_version:  # Version is not found in dest
            logging.error(f"{version} does not exist in {dest}")
            return False
    return True


def rename_versions(versions: list):  # Rename versions in dest directory
    dest = "D:\\FusWs"
    curr_versions = os.listdir(dest)
    for version in versions:
        curr_version = [s for s in curr_versions if version in s]
        if curr_version and curr_version[0] != version:
            os.rename(os.path.join(dest, curr_version[0]), os.path.join(dest, version))
            logging.info(f"Renamed folder: {curr_version[0]}")


# Define inputs
parser = argparse.ArgumentParser(description="Process data with specified options")
parser.add_argument("-i", "--input_csv", required=True, help="The input file of requirements as CSV")
parser.add_argument("-v", "--vendor", required=True, help="The name of the MRI vendor")
parser.add_argument("-m", "--mr_model", required=True, help="The model of the MRI")
parser.add_argument("-f", "--field", required=True, help="The strength of the field")
parser.add_argument("-n", "--input_not_null", required=True, help="The input file that contains file to ignore when "
                                                                  "setting null values")
parser.add_argument("--versions", nargs="+", required=True, help="All fus versions")
parser.add_argument("-b", "--backup", required=False, help="Backup files before execution")
args = parser.parse_args()

#  Read inputs
req: str = args.input_csv
logging.info(f"Req file taken from: {req}")
if not os.path.exists(req):
    logging.error(f"Requirements path does not exist")
    raise Exception(f"Requirements path does not exist")
files_to_skip_path: str = args.input_not_null
logging.info(f"Files to skip data was taken from: {files_to_skip_path}")
files_to_skip_data: dict = get_files_to_skip(files_to_skip_path)
vendor: str = args.vendor
logging.info(f"Vendor name is: {vendor}")
mr_model: str = args.mr_model
logging.info(f"MRI model is: {mr_model}")
field_strength: str = args.field
logging.info(f"Field strength is: {field_strength}")
ls_fus_versions: list[str] = args.versions
logging.info(f"Selected FUS: {ls_fus_versions}")
backup_option = args.backup
df_req: pd.DataFrame = pd.read_csv(req)
df_req = df_req.replace(np.nan, 'None')
df_req_col_names = {'Vendor', 'MR', 'FieldStrength', 'path', 'section', 'ini key', 'value',
                    'added/updated in Xcom Version'}
if not df_req_col_names.issubset(df_req.columns.str.strip()):  # Verify col names are valid
    logging.error("Invalid columns names in requirements file")
    raise Exception("Invalid columns names in requirements file")

if df_req.duplicated().any():  # Check for duplications in req file
    logging.error(f"There are duplications in requirement file: {df_req[df_req.duplicated()]}")
    raise Exception("There are duplications in requirement file")

# Check versions exist in D:\FusWs
if not check_versions_exist(ls_fus_versions):
    logging.error("Not all versions to check exist in D:\\FusWs, check log file")
    raise Exception("Not all versions to check exist in D:\\FusWs, check log file")

# Filtering req file according to input parameters
df_filtered = df_req[(df_req['Vendor'] == vendor) & (df_req['MR'] == mr_model)
                     & (df_req['FieldStrength'] == field_strength)
                     & (df_req['path'].str.contains('|'.join(ls_fus_versions)))]

if df_filtered.empty:
    logging.error(f"Requirements are not found for {vendor}, {mr_model}, {field_strength}, {ls_fus_versions}")
    raise Exception(f"Requirements are not found for {vendor}, {mr_model}, {field_strength}, {ls_fus_versions}")

# Remove the endings of versions
rename_versions(ls_fus_versions)

if backup_option is None:  # Backup versions by default
    # Backup FUS versions from src dir to a given dest dir
    print("Start backup")
    dest_dir = os.path.join(script_dir, "Backup")
    src_dir = "D:\\FusWs"
    backup = Backup(src_dir, dest_dir)
    logging.info(f"Start to backup from: {src_dir} to {dest_dir}")
    for fus_folder in ls_fus_versions:  # Iterating specific FUS versions and backup each version to src_dir
        logging.info(f"Start to backup {fus_folder}")
        backup.backup_version(fus_folder)
    print("Finished backup")
    logging.info("Backup is completed")

# Parse requirements into structured data
logging.info("Start parsing")
print("Start parsing")
requirements = Requirements(mr_model, field_strength, vendor, df_filtered)
print("Finished parsing")
logging.info("Parsing is completed")

# Set default values to files
for fus_folder in ls_fus_versions:
    print(f"Start setting default values for {fus_folder}")
    logging.info(f"Start to set default values to: {fus_folder}")
    DefaultValue.set_default_values_in_folder(os.path.join("D:\\FusWs", fus_folder), files_to_skip_data)
    print(f"Finished setting default values for {fus_folder}")
print("Finished setting default values")
logging.info("Set default values is completed")

# Require input from user to proceed
while True:
    ans: str = input(f"Run XCom with these parameters and write ok to continue: {vendor}, {mr_model}, "
                     f"{field_strength}, {ls_fus_versions}")
    if ans.lower() == 'ok':
        print("Proceeding to validation")
        break
    else:
        print("Waiting for OK...")
        continue

# Validate after XCom run
for fus_folder in ls_fus_versions:
    print(f"Start validating {fus_folder}")
    logging.info(f"Start to validate folder: {fus_folder}")
    requirements.validate(os.path.join("D:\\FusWs", fus_folder), files_to_skip_data)
    print(f"Finished validating {fus_folder}")
    logging.info(f"Finished validating: {fus_folder}")
print("Finish validation")

# Output
print("Start outputting")
logging.info("Start outputting")
requirements.output(script_dir, ls_fus_versions)
print("Finished outputting")

# Restore
if backup_option is None:  # Restore files by default
    print("Start restoring")
    logging.info(f"Start to restore from {dest_dir} to {src_dir}")
    for fus_folder in ls_fus_versions:
        logging.info(f"Start to restore {fus_folder}")
        backup.restore(fus_folder)
    logging.info("Restore is completed")
    print("Finished restoring")

print(f"The Run for these parameters is completed: {vendor}, {mr_model}, "
      f"{field_strength}, {ls_fus_versions}")
time.sleep(5)

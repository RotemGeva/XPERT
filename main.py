import logging
from datetime import datetime
from Backup import Backup
from Requirements import Requirements
import argparse
import pandas as pd
import os
import numpy as np
import json5 as json
from DefaultValue import DefaultValue


def define_log() -> None:  # Log settings
    script_dir = os.path.dirname(os.path.realpath(__file__))
    log_path = os.path.join(script_dir, 'Logs')
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    logging.basicConfig(level=logging.DEBUG,
                        filename=os.path.join(log_path, f"log_{datetime.now().strftime('%m-%d_%H-%M')}.log"),
                        filemode='w',
                        format="%(asctime)s - %(funcName)20s() - %(levelname)s - %("
                               "message)s")


def get_files_to_skip(path: str) -> dict:  # Get all skipped files from a given path of an external file
    try:
        with open(path, 'r') as file:
            json_data = json.load(file)
            return json_data
    except Exception:
        logging.error(f"Failed to read JSON file with skipped keys from {path}")
        raise Exception("Failed to read JSON file with skipped keys")


def check_versions_exist(versions: list) -> None:  # Verify given versions exist in dest
    dest = "D:\\FusWs"
    curr_versions = os.listdir(dest)
    for version in versions:
        curr_version = [s for s in curr_versions if version in s]
        if not curr_version:  # Version is not found in dest
            logging.error(f"{version} does not exist in {dest}")
            raise Exception("Not all versions to check, exist in D:\\FusWs, check log file")


def rename_versions(versions_to_check: list) -> None:  # Rename versions in dest directory
    dest = "D:\\FusWs"
    versions_in_dir = os.listdir(dest)
    for version in versions_to_check:
        curr_version = [s for s in versions_in_dir if version+"_" in s]
        if curr_version and curr_version[0] != version:
            os.rename(os.path.join(dest, curr_version[0]), os.path.join(dest, version))
            logging.info(f"Renamed folder: {curr_version[0]}")


def read_inputs(arguments: argparse.Namespace) -> tuple:
    req_path: str = arguments.input_csv
    logging.info(f"Req file taken from: {req_path}")
    if not os.path.exists(req_path):
        logging.error(f"Requirements path does not exist")
        raise Exception(f"Requirements path does not exist")
    files_to_skip_path: str = arguments.input_not_null
    logging.info(f"Files to skip was taken from: {files_to_skip_path}")
    files_to_skip_data: dict = get_files_to_skip(files_to_skip_path)
    vendor: str = arguments.vendor
    logging.info(f"Vendor name is: {vendor}")
    mr_model: str = arguments.mr_model
    logging.info(f"MRI model is: {mr_model}")
    field_strength: str = arguments.field
    logging.info(f"Field strength is: {field_strength}")
    ls_fus_versions: list[str] = arguments.versions
    logging.info(f"Selected FUS: {ls_fus_versions}")
    backup_option: str = arguments.backup
    df_req: pd.DataFrame = pd.read_csv(req_path)
    df_req = df_req.replace(np.nan, 'None')
    verify_col_names(df_req)
    check_duplications(df_req)
    check_versions_exist(ls_fus_versions)
    return df_req, files_to_skip_data, vendor, mr_model, field_strength, ls_fus_versions, backup_option


def verify_col_names(df_req) -> None:
    df_req_col_names = {'Vendor', 'MR', 'FieldStrength', 'path', 'section', 'ini key', 'value',
                        'added/updated in Xcom Version'}
    if not df_req_col_names.issubset(df_req.columns.str.strip()):  # Verify col names are valid
        logging.error("Invalid columns names in requirements file")
        raise Exception("Invalid columns names in requirements file")


def define_inputs() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process data with specified options")
    parser.add_argument("-i", "--input_csv", required=True, help="The input file of requirements as CSV")
    parser.add_argument("-v", "--vendor", required=True, help="The name of the MRI vendor")
    parser.add_argument("-m", "--mr_model", required=True, help="The model of the MRI")
    parser.add_argument("-f", "--field", required=True, help="The strength of the magnetic field")
    parser.add_argument("-n", "--input_not_null", required=True,
                        help="The input file that contains file to ignore when "
                             "setting null values")
    parser.add_argument("--versions", nargs="+", required=True, help="All fus versions")
    parser.add_argument("-b", "--backup", required=False, help="Backup files before execution")
    arguments = parser.parse_args()
    return arguments


def check_duplications(df_req) -> None:
    if df_req.duplicated().any():  # Check for duplications in req file
        logging.error(f"There are duplications in requirement file: {df_req[df_req.duplicated()]}")
        raise Exception("There are duplications in requirement file")


def filter_req(df_req: pd.DataFrame) -> pd.DataFrame:  # Filtering req file according to input parameters
    df_filtered = df_req[(df_req['Vendor'] == vendor) & (df_req['MR'] == mr_model)
                         & (df_req['FieldStrength'] == field_strength)
                         & (df_req['path'].apply(lambda x: any(s + '\\' in x for s in ls_fus_versions)))]
    #.str.contains('|'.join(ls_fus_versions)))
    if df_filtered.empty:
        logging.error(f"Requirements are not found for {vendor}, {mr_model}, {field_strength}, {ls_fus_versions}")
        raise Exception(f"Requirements are not found for {vendor}, {mr_model}, {field_strength}, {ls_fus_versions}")
    return df_filtered


def backup_versions() -> Backup:  # Backup FUS versions from src dir to a given dest dir
    script_dir: str = os.path.dirname(os.path.realpath(__file__))
    print("Start backup")
    dest_dir = os.path.join(script_dir, "Backup")
    src_dir = "D:\\FusWs"
    backup: Backup = Backup(src_dir, dest_dir)
    logging.info(f"Start to backup from: {src_dir} to {dest_dir}")
    for fus_folder in ls_fus_versions:  # Iterating specific FUS versions and backup each version to src_dir
        logging.info(f"Start to backup {fus_folder}")
        backup.backup_version(fus_folder)
    print("Finished backup")
    logging.info("Backup is completed")
    return backup


def parse_req() -> Requirements:  # Parse requirements into structured data
    logging.info("Start parsing")
    print("Start parsing")
    requirements: Requirements = Requirements(mr_model, field_strength, vendor, df_filtered)
    print("Finished parsing")
    logging.info("Parsing is completed")
    return requirements


def set_default_values(ls_fus_versions: list) -> None:  # Set default values to files
    for fus_folder in ls_fus_versions:
        print(f"Start setting default values for {fus_folder}")
        logging.info(f"Start to set default values to: {fus_folder}")
        DefaultValue.set_default_values_in_folder(os.path.join("D:\\FusWs", fus_folder), files_to_skip_data)
        print(f"Finished setting default values for {fus_folder}")
    print("Finished setting default values")
    logging.info("Set default values is completed")


def validate_versions(ls_fus_versions: list) -> None:  # Validate after XCom run
    for fus_folder in ls_fus_versions:
        print(f"Start validating {fus_folder}")
        logging.info(f"Start to validate folder: {fus_folder}")
        requirements.validate(os.path.join("D:\\FusWs", fus_folder), files_to_skip_data)
        print(f"Finished validating {fus_folder}")
        logging.info(f"Finished validating: {fus_folder}")
    print("Finish validation")


def create_output(requirements: Requirements, ls_fus_versions: list) -> None:
    script_dir: str = os.path.dirname(os.path.realpath(__file__))
    print("Start outputting")
    logging.info("Start outputting")
    requirements.output(script_dir, ls_fus_versions)
    print("Finished outputting")


def restore_versions(backup: Backup):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    dest_dir = os.path.join(script_dir, "Backup")
    src_dir = "D:\\FusWs"
    print("Start restoring")
    logging.info(f"Start to restore from {dest_dir} to {src_dir}")
    for fus_folder in ls_fus_versions:
        logging.info(f"Start to restore {fus_folder}")
        backup.restore(fus_folder)
    logging.info("Restore is completed")
    print("Finished restoring")


def read_user_response() -> None:
    while True:
        ans: str = input(f"Run XCom with these parameters and write ok to continue: {vendor}, {mr_model}, "
                         f"{field_strength}, {ls_fus_versions}")
        if ans.lower() == 'ok':
            print("Proceeding to validation")
            break
        else:
            print("Waiting for OK...")
            continue


# Main flow
def map_versions(selected_versions: list) -> dict:
    logging.info(f"Starts mapping the following versions: {selected_versions}")
    actual_versions = os.listdir("D:\\FusWs")
    selected_versions.remove("CommonMR")
    selected_versions = [item+"_" for item in selected_versions]
    mapping = {key: [x for x in actual_versions if key in x][0] for key in selected_versions}
    mapping = {key[:-len("_")]: value for key, value in mapping.items() if key.endswith("_")}
    logging.info(f"Created the following mapping: {mapping}")
    return mapping


def restore_folder_names(mapping: dict, backup: Backup) -> None:
    for fus_version in mapping.keys():
        os.rename(os.path.join(backup.src_dir, fus_version), os.path.join(backup.dest_dir, mapping[fus_version]))


try:
    define_log()
    args = define_inputs()
    df_req, files_to_skip_data, vendor, mr_model, field_strength, ls_fus_versions, backup_option = read_inputs(args)
    df_filtered = filter_req(df_req)
    mapping = map_versions(ls_fus_versions)
    rename_versions(ls_fus_versions)
    if backup_option is None:  # Backup versions by default
        backup_details = backup_versions()
    requirements = parse_req()
    set_default_values(ls_fus_versions)
    read_user_response()
    validate_versions(ls_fus_versions)
    create_output(requirements, ls_fus_versions)
    if backup_option is None:  # Restore files by default
        # noinspection PyUnboundLocalVariable
        restore_versions(backup_details)
        restore_folder_names(mapping, backup_details)
    print(f"The Run for these parameters was completed: {vendor}, {mr_model}, "
          f"{field_strength}, {ls_fus_versions}")
except Exception as err:
    logging.error(f"err: {err}, type: {type(err)}")
    raise Exception("General exception")

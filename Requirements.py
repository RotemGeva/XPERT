import logging
from datetime import datetime
import pandas as pd
import os
import json5 as json
import pyiniconfig
from KeyResult import KeyResult
from KeyResult import Result
from DefaultValue import DefaultValue


class Requirements:
    def __init__(self, mr_model, field, vendor, df: pd.DataFrame):
        self._ini_files: IniFiles = IniFiles()
        self._json_files: JsonFiles = JsonFiles()
        self._mr_model = mr_model
        self._field = field
        self._vendor = vendor
        ls_path = (pd.unique(df['path'])).tolist()
        for path in ls_path:
            file_ext = os.path.splitext(path)[1].lower()
            match file_ext:
                case ".ini":
                    self._ini_files.add_file(path, df)
                case ".json":
                    self._json_files.add_file(path, df)

    @property
    def ini_files(self):
        return self._ini_files

    @property
    def json_files(self):
        return self._json_files

    def validate(self, version_path, files_to_skip: dict):
        for root, dirs, files in os.walk(version_path, topdown=False):
            for filename in files:
                curr_path = os.path.join(root, filename)
                if not curr_path.__contains__('Local') and not curr_path.__contains__('Iniguard\\Log'):
                    # Without root: D:\FusWs - Fus-7.44\Site\SiteInifiles\example.ini
                    path_no_root = os.sep.join(curr_path.split(os.sep)[2:])
                    extension = os.path.splitext(filename)[1]
                    path_no_version = os.sep.join(path_no_root.split(os.sep)[1:])  # Site\SiteInifiles\example.ini
                    sections_to_skip = files_to_skip.get(path_no_version)
                    match extension:
                        case '.ini':
                            self._ini_files.validate_file(curr_path, path_no_root, sections_to_skip)
                        case '.json':
                            self._json_files.validate_file(curr_path, path_no_root, sections_to_skip)

    def output(self, dest_dir, versions: list):
        ls_of_df = []
        df_ini_files = self._ini_files.create_df()
        df_json_files = self._json_files.create_df()
        ls_of_df.append(df_ini_files)
        ls_of_df.append(df_json_files)
        if self._mr_model.__contains__('/'):
            self._mr_model = self._mr_model.replace('/', '_')
        filename = self._vendor + '_' + self._mr_model + '_' + self._field + '_' + "_".join(
            versions) + '_' + datetime.now().strftime('%m%d_%H%M') + '.csv'
        if not os.path.exists(os.path.join(dest_dir, 'Results')):
            os.makedirs(os.path.join(dest_dir, 'Results'))
        pd.concat(ls_of_df).to_csv(path_or_buf=os.path.join(dest_dir, 'Results', filename), index=False, na_rep='None')
        logging.info("Output is in: " + os.path.join(dest_dir, "Results"))


class Files:
    DELIMITER_FOR_LIST = ';'

    def __init__(self):
        self._files: dict = {}

    def __getitem__(self, item):
        return self._files[item]

    def create_df(self):
        col_names = ['File', 'Section', 'Key', 'Expected', 'Actual', 'Status']
        df = pd.DataFrame(columns=col_names)  # Create empty df
        for path in self._files:
            for section in self._files[path]:
                for key in self._files[path][section]:
                    key_result = self._files[path][section][key]
                    if type(key_result.actual) is str:
                        if key_result.actual.startswith('-'):
                            key_result.actual = " " + key_result.actual
                    if type(key_result.expected) is str:
                        if key_result.expected.startswith('-'):
                            key_result.expected = " " + key_result.expected
                    new_row = {'File': path, 'Section': section, 'Key': key, 'Expected': key_result.expected,
                               'Actual': key_result.actual,
                               'Status': key_result.result.name}
                    df.loc[len(df)] = new_row
        return df

    def add_file(self, path, df: pd.DataFrame):
        df_path = df[df['path'] == path]
        ls_sections = (pd.unique(df_path['section'])).tolist()
        sections = {}
        for section in ls_sections:
            sections[section] = {}
            df_section = df[(df['path'] == path) & (df['section'] == section)]
            ls_keys = (pd.unique(df_section['ini key'])).tolist()
            for key in ls_keys:
                expected_val = df_section.loc[df_section['ini key'] == key, 'value'].item()
                if str(expected_val).__contains__(self.DELIMITER_FOR_LIST):
                    ls_expected_val = expected_val.split(self.DELIMITER_FOR_LIST)
                    sections[section][key] = KeyResult(ls_expected_val)
                else:
                    sections[section][key] = KeyResult(expected_val)
        self._files[path] = sections

    def validate_file(self, full_file_path, no_root_file_path, sections_to_skip):
        pass

    def handle_key(self, file_path, section, key, actual):
        logging.info(f"Check key {key} in section {section} in file {file_path}")
        key_result = self._files.get(file_path, {}).get(section, {}).get(key)
        if key_result is not None:  # Key is found in data
            logging.info(f"key {key} in section {section} in file {file_path} exists in data")
            key_result.validate(actual)
        else:  # Key is not in data
            if actual != DefaultValue.DEFAULT_VALUE_JSON and actual != DefaultValue.DEFAULT_VALUE_INI:
                logging.info(f"Adding key {key} in section {section} in file {file_path} to data")
                self._files.setdefault(file_path, {}).setdefault(section, {}).setdefault(key, KeyResult(
                    DefaultValue.DEFAULT_VALUE_INI, actual=actual,
                    result=Result.UNEXPECTED_MODIFICATION))


class IniFiles(Files):
    def validate_file(self, full_file_path, no_root_file_path, sections_to_skip: dict):
        config = pyiniconfig.IniConfig(full_file_path)
        for section in config.get_sections():
            options = config.get_options(section_name=section)
            for key in options:
                if sections_to_skip is None:  # There are no keys to skip in this file
                    try:
                        self.handle_key(no_root_file_path, section, key, options[key])
                    except Exception as err:
                        logging.warning(f"Failed to validate: {no_root_file_path}, {section}, {key} - err: {err}, "
                                        f"type: {type(err)}")
                elif key not in sections_to_skip.get(section, {}): # This key is not a skipped key
                    try:
                        self.handle_key(no_root_file_path, section, key, options[key])
                    except Exception as err:
                        logging.warning(f"Failed to validate: {no_root_file_path}, {section}, {key} - err: {err},"
                                        f" type: {type(err)}")


class JsonFiles(Files):
    def validate_file(self, full_file_path, no_root_file_path, sections_to_skip):
        try:
            with open(full_file_path, "r") as f:
                file_data = f.read()
                json_data = json.loads(file_data)
        except Exception as err:
            logging.warning(f"Failed to open: {no_root_file_path}, err: {err}, type: {type(err)}")
        for tag, actual in JsonFiles.extract_tags_and_values(json_data):
            section = tag[:tag.rfind(".")]
            key = tag.split('.')[-1]
            if sections_to_skip is None:
                try:
                    self.handle_key(no_root_file_path, section, key, actual)
                except Exception as err:
                    logging.warning(f"Failed to validate: {no_root_file_path}, {section}, {key} - err: {err}, "
                                    f"type: {type(err)}")
            elif key not in sections_to_skip.get(section, {}):
                try:
                    self.handle_key(no_root_file_path, section, key, actual)
                except Exception as err:
                    logging.warning(f"Failed to validate: {no_root_file_path}, {section}, {key} - err: {err},"
                                    f" type: {type(err)}")

    @staticmethod
    def extract_tags_and_values(data: dict, prefix=""):
        results = []
        if isinstance(data, dict):
            for key, value in data.items():
                new_prefix = f"{prefix}.{key}" if prefix else key
                if isinstance(value, list):
                    results.append((new_prefix, value))
                else:
                    results.extend(JsonFiles.extract_tags_and_values(value, new_prefix))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_prefix = f"{prefix}[{i}]" if prefix else f"[{i}]"
                if isinstance(item, list):
                    results.append((new_prefix, item))
                else:
                    results.extend(JsonFiles.extract_tags_and_values(item, new_prefix))
        else:
            results.append((prefix, data))
        return results

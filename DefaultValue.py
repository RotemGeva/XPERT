import logging
import os
import json5 as json


class DefaultValue:
    DEFAULT_VALUE_INI = 'null'
    DEFAULT_VALUE_JSON = 12345

    @staticmethod
    def set_default_values_in_folder(folder_path: str, files_to_skip: dict):
        for root, dirs, files in os.walk(folder_path, topdown=False):
            for filename in files:
                file_path = os.path.join(root, filename)
                path_no_prefix = os.sep.join(file_path.split(os.sep)[3:])
                sections_to_skip = files_to_skip.get(path_no_prefix)
                extension = os.path.splitext(filename)[1]
                match extension:
                    case '.ini':
                        if sections_to_skip is None:  # There are no skipped keys in file
                            DefaultValueIni.set_default_values_ini(file_path)
                        else:
                            DefaultValueIni.set_default_values_ini_skipped_keys(file_path, sections_to_skip)
                    case '.json':
                        DefaultValueJson.set_default_values_json(file_path)


class DefaultValueIni:
    @staticmethod
    def set_default_values_ini_skipped_keys(file_path: str, skipped_sections: dict):
        try:
            with open(file_path, 'r') as file:
                content = file.read()
        except Exception as err:
            logging.warning(f"Could not read file: {file_path}, err: {err}, type: {type(err)}")
        new_lines = []
        in_keep_section = False
        for line in content.splitlines():
            if line.startswith("["):
                section_name = line.strip()[1:-1]
                in_keep_section = section_name in skipped_sections
                new_lines.append(line)
            elif in_keep_section:  # Line is a skipped section
                if line.startswith(tuple(skipped_sections[section_name])):  # Check if line starts with a kept key
                    new_lines.append(line)
                else:  # Line does not start with a skipped key
                    if not line.startswith(';') and ';' not in line and '=' in line:  # Line is not a comment
                        key, value = line.split('=', 1)
                        new_lines.append(f"{key} = " + DefaultValue.DEFAULT_VALUE_INI)
            else:  # Line is not a skipped section
                if not line.startswith(';') and ';' not in line and '=' in line:
                    key, value = line.split('=', 1)
                    # Set all values to default values outside a skipped  sections
                    new_lines.append(f"{key} = " + DefaultValue.DEFAULT_VALUE_INI)
        new_content = "\n".join(new_lines)
        try:
            with open(file_path, 'w') as file:
                file.write(new_content)
        except Exception as err:
            logging.warning(f"Could not read file: {file_path}, err: {err}, type: {type(err)}")

    @staticmethod
    def set_default_values_ini(file_path: str):
        try:
            with open(file_path, "r") as f:
                content = f.read()
        except Exception as err:
            logging.warning(f"Could not read file: {file_path}, err: {err}, type: {type(err)}")
        lines = []
        for line in content.splitlines():
            if (";" not in line or not line.startswith(';')) and "=" in line:
                key, value = line.split('=', 1)
                lines.append(f"{key} = " + DefaultValue.DEFAULT_VALUE_INI)
            elif line.startswith('['):
                lines.append(line)
        new_content = "\n".join(lines)
        try:
            with open(file_path, 'w') as f:
                f.write(new_content)
        except Exception as err:
            logging.warning(f"Could not write to file: {file_path}, err: {err}, type: {type(err)}")


class DefaultValueJson:
    @staticmethod
    def set_default_values_json(file_path: str):
        try:
            with open(file_path, 'r') as file_to_read:
                file_data = file_to_read.read()
        except Exception as err:
            logging.info(f"Could not read file: {file_path}, error: {err}, type: {type(err)}")
        json_data = json.loads(file_data)
        DefaultValueJson.set_dict_to_zeros(json_data)
        try:
            with open(file_path, 'w') as file_to_write:
                json.dump(json_data, file_to_write, indent=4)
        except Exception as err:
            logging.info(f"Could not write to file: {file_path}, error: {err}, type: {type(err)}")

    @staticmethod
    def set_dict_to_zeros(dictionary: dict):
        for key, value in dictionary.items():
            if isinstance(value, dict):
                DefaultValueJson.set_dict_to_zeros(value)
            else:
                dictionary[key] = DefaultValue.DEFAULT_VALUE_JSON

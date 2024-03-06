import logging
import os
import shutil


class Backup:
    def __init__(self, src_dir, dest_dir):
        self._src_dir = src_dir
        self._dest_dir = dest_dir

    def backup_version(self, version_folder_name: str):
        # Backup folder does not exist yet
        if not os.path.exists(self._dest_dir):
            os.makedirs(self._dest_dir)
            logging.info(f"Created backup folder in: {self._dest_dir}")
        else:
            logging.info(f"Backup folder already exists in: {self._dest_dir}")
        version_folder_path = os.path.join(self._src_dir, version_folder_name)
        version_backup_folder = os.path.join(self._dest_dir, version_folder_name)
        # Version folder does not already exist in backup folder
        if not os.path.exists(version_backup_folder):
            os.makedirs(os.path.join(self._dest_dir, version_folder_name))
            ext_ini = '.ini'
            ext_json = '.json'
            for root, dirs, files in os.walk(os.path.join(self._src_dir, version_folder_path)):
                for dir in dirs:
                    dest_subdir = os.path.join(version_backup_folder, os.path.relpath(root, version_folder_path), dir)
                    os.makedirs(dest_subdir, exist_ok=True)
                for filename in files:
                    if filename.endswith(ext_ini) or filename.endswith(ext_json):
                        src_file = os.path.join(root, filename)
                        dst_file = os.path.join(version_backup_folder, os.path.relpath(src_file, version_folder_path))
                        try:
                            shutil.copy2(src_file, dst_file)
                        except Exception as err:
                            logging.warning(f"Failed to backup: {src_file}, error: {err} type: {type(err)}")
            logging.info(f"Backup completed for: {version_folder_name}")
        else:
            logging.info(f"Backup folder of {version_folder_name} already exists")

    def restore(self, version_folder_name: str):
        for root, dirs, files in os.walk(os.path.join(self._dest_dir, version_folder_name), topdown=False):
            for file in files:
                dst_rel = os.path.join(self._src_dir, os.path.relpath(root, self._dest_dir), file)
                src_rel = os.path.join(self._dest_dir, os.path.relpath(root, self._dest_dir), file)
                try:
                    shutil.copy(src_rel, dst_rel)
                except Exception as err:
                    logging.warning(f"Failed to restore {src_rel}, err: {err}, type: {type(err)}")


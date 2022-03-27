
import os
import shutil
from utils.logging import get_logger

logger = get_logger(__name__)

def move_files_from_dir(source_folder: str, destination_folder: str, extension_filter: str = '.json'):
    # fetch all files
    for file_name in os.listdir(source_folder):
        _, file_extension = os.path.splitext(file_name)
        if file_extension == extension_filter:
            if not os.path.exists(destination_folder):
                os.makedirs(destination_folder)
            # construct full file path
            source = os.path.join(source_folder, file_name)
            destination = os.path.join(destination_folder, file_name) 
            # move only files
            if os.path.isfile(source):
                shutil.move(source, destination)
                logger.info(f'Moved: {source} to {destination}')
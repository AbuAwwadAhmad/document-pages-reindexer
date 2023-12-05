import os
import shutil
import subprocess
import tempfile
import traceback
from uuid import uuid4

from docx2pdf import convert


def create_tmp_folder(file_name):
    folder_path = f'/tmp/{file_name}-{uuid4()}'
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder '{folder_path}' created successfully.")
    else:
        print(f"Folder '{folder_path}' already exists.")
    return folder_path


def remove_tmp_folder(folder_path):
    try:
        shutil.rmtree(folder_path)
        print(f"File '{folder_path}' removed successfully.")
    except FileNotFoundError:
        print(f"File '{folder_path}' not found.")
    except Exception as e:
        traceback.print_exc()
        print(f"Error removing file '{folder_path}': {str(e)}")


def change_extension_to_pdf(file_path):
    file_path, _ = os.path.splitext(file_path)
    file_path += '.pdf'
    return file_path


def convert_docx_to_pdf(input_path):
    print("Converting docx to pdf")
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_path = temp_dir.name
    command = f'libreoffice --convert-to pdf {input_path} --outdir {os.path.dirname(input_path)} '
    subprocess.run(command, shell=True, text=True, check=True, env={"HOME": temp_dir_path})


def convert_docx_to_pdf_windows(input_path):
    try:
        convert(input_path)
        output_path = os.path.splitext(input_path)[0] + '.pdf'
        print(f"Conversion successful. PDF saved at: {output_path}")
        return output_path
    except Exception as e:
        print(f"Error during conversion: {e}")
        return None

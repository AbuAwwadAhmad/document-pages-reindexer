import fitz

from utils import file_utils
from utils.aws import s3


def process_document(s3_file_url, file_id, file_name):
    try:
        tmp_folder = file_utils.create_tmp_folder(file_name)
        local_file_path = f"{tmp_folder}/{file_name}"
    except Exception as e:
        raise e

    local_file_path = s3.download_file_from_s3(s3_file_url, file_id, local_file_path)

    if local_file_path is None:
        raise Exception(f"Failed to download document from {s3_file_url} for file id {file_id}")

    if s3_file_url.lower().endswith('.pdf'):
        processed_data = process_pdf(pdf_path=local_file_path, file_id=file_id)
    elif s3_file_url.lower().endswith('.docx'):
        file_utils.convert_docx_to_pdf(local_file_path)
        local_file_path = file_utils.change_extension_to_pdf(local_file_path)
        processed_data = process_pdf(pdf_path=local_file_path, file_id=file_id)
    else:
        raise Exception(f"Unsupported file format for {s3_file_url}")

    try:
        file_utils.remove_tmp_folder(tmp_folder)
    except Exception as e:
        raise e
    return processed_data


def process_pdf(pdf_path, file_id):
    try:
        pdf_document = fitz.open(pdf_path)
        page_count = pdf_document.page_count
        pages_data = []

        for page_number in range(page_count):
            page = pdf_document.load_page(page_number)
            text = page.get_text()
            pages_data.append({'text': text, 'page_number': page_number + 1})

        return {'pages': pages_data}
    except Exception as e:
        raise Exception(f"Failed processing document with file id: ", file_id, " due to ", e)

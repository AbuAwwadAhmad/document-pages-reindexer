from services import file_processor, opensearch_service, content_service


def index_documents():
    approved_documents = content_service.get_approved_documents()

    for db_record in approved_documents:
        try:
            s3_file_url = db_record['url']
            file_id = db_record['id']
            file_name = db_record['name']
            pages_data = file_processor.process_document(s3_file_url, file_id, file_name)
            opensearch_service.index_to_opensearch(db_record, pages_data)

        except Exception as e:
            print(e)
    return


if __name__ == "__main__":
    index_documents()

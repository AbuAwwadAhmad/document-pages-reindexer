from opensearchpy import OpenSearch, TransportError

from utils import constantsTest as constants
# from utils import constants
from utils.aws import secret_manager
import json


def index_to_opensearch(data, pages_data):
    try:
        os_client = OpenSearch(
            hosts=[constants.OPEN_SEARCH_HOST],
            http_compress=True,
            http_auth=(secret_manager.get_secret(constants.OPEN_SEARCH_SECRET, constants.OPEN_SEARCH_USERNAME),
                       secret_manager.get_secret(constants.OPEN_SEARCH_SECRET, constants.OPEN_SEARCH_PASSWORD)),
            use_ssl=True,
            verify_certs=True,
            ssl_show_warn=False,
        )

        data_for_bulk = []
        i = 0
        for page in pages_data['pages']:
            page_number = page['page_number']
            text = page['text']
            pages = json.loads(data['pages'])

            continue_var = False
            for page_record in pages:
                if page_record.get('page_number', 0) is None or int(page_number) != int(
                        page_record.get('page_number', 0)):
                    continue_var = True
                else:
                    continue_var = False
                    break

            if text is None or text == '' or continue_var:
                i = i + 1
                continue

            document_id = pages[i]['id']

            print(f'Page Number: {page_number}, Document ID: {document_id}')

            document_body = {
                'document_id': document_id,
                'file': {
                    'name': data['name'],
                    'url': data['url'],
                    'extension': data['extension'],
                    'type': 'OTHER' if data['file_type'] is None else data['file_type'],
                    'id': data['id'],
                    'user_id': data['user_id'],
                },
                'course': {
                    'id': data['uc.id'],
                    'title': data['title'],
                    'shorten_title': data['shorten_title'],
                },
                'university': {
                    'id': data['u.id'],
                    'name': data['u.name'],
                    'shorten_name': data['shorten_name'],
                },
                'page_number': page_number,
                'page_name': pages[i]['name'],
                'page_thumbnail': pages[i]['thumbnail_uri'],
                'page_slug': str(document_id) + '-' + pages[i]['slug'],
                'searchable_field': text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
                .replace('', ' ').replace('', ' '),
                'id': document_id
            }
            i = i + 1

            data_for_bulk.extend([
                {"index": {"_index": constants.index_name, "_id": document_id}},
                document_body
            ])

        try:
            if not data_for_bulk:
                return

            response = os_client.bulk(body=data_for_bulk, refresh=True)
            if response["errors"]:
                for item in response["items"]:
                    if "index" in item and "error" in item["index"]:
                        print(f"Error indexing document: {item['index']['error']}")
            return response
        except TransportError as e:
            print(f"TransportError: {e}")
            raise e
        except Exception as e:
            print(f"Error: {e}")
            raise e
        finally:
            os_client.close()

    except ConnectionError as ce:
        print(f"Failed to connect to Elasticsearch. Error: {ce}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

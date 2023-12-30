import pymysql

from utils import constantsTest as constants
# from utils import constants
from utils.aws import secret_manager


def get_db_connection():
    return pymysql.connect(
        host=secret_manager.get_secret(constants.MYSQL_SECRET, constants.MYSQL_HOST),
        user=secret_manager.get_secret(constants.MYSQL_SECRET, constants.MYSQL_USER),
        password=secret_manager.get_secret(constants.MYSQL_SECRET, constants.MYSQL_PASSWORD),
        database=constants.MYSQL_DATABASE,
        port=3306,
        cursorclass=pymysql.cursors.DictCursor
    )


def get_approved_documents():
    connection = get_db_connection()
    cursor = connection.cursor()

    try:
        query = """SELECT f.id, f.name,  f.url, f.user_id, f.extension, f.file_type, f.pages_count, 
                uc.title, uc.id, uc.shorten_title, u.id, u.name, u.shorten_name,
                CASE WHEN f.file_type = 'LECTURE_NOTES' THEN '#004D73' WHEN f.file_type = 'EXAM_NOTES' THEN '#386C5F' WHEN f.file_type = 'SUMMARY' THEN '#FF3A30' WHEN f.file_type = 'PRACTICE_MATERIALS' THEN '#009CFE' WHEN f.file_type = 'TEXT_BOOK' THEN '#010FFF' WHEN f.file_type = 'ASSIGNMENT' THEN '#5D5FEF' WHEN f.file_type = 'TUTORIAL' THEN '#EF5DA8' WHEN f.file_type = 'CLASS_NOTES' THEN '#5CB6C2' WHEN f.file_type = 'REPORT' THEN '#E86427' WHEN f.file_type = 'ESSAY' THEN '#48C690' ELSE '#0073ba' END AS file_color, 
                CONCAT('[', GROUP_CONCAT( 
                JSON_OBJECT('id', fp.id, 'slug', fp.slug, 'name', fp.name, 'page_number', fp.page_number,
                'thumbnail_uri', fp.thumbnail_uri) ), ']') as pages 
                FROM file f 
                LEFT JOIN file_page fp on f.id = fp.file_id 
                LEFT JOIN university_course_files ucf on f.id = ucf.file_id 
                LEFT JOIN university_course uc on ucf.university_course_id = uc.id 
                LEFT JOIN university u on uc.university_id = u.id 
                WHERE f.status = 'APPROVED' 
                   AND f.deleted = 0 
                   AND f.extension in ('pdf', 'docx') 
                    AND f.created_at > '2023-08-01 14:31:26' 
                    AND f.processing_status > 'COMPLETED' 
                GROUP BY f.id 
                ORDER BY f.id DESC """
        cursor.execute(query)

        return cursor.fetchall()
    finally:
        cursor.close()
        connection.close()

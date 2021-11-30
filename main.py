import pymysql
from pymysql.err import OperationalError

pymysql_config = {
    'user': 'cloudFunction',
    'password': '$D>rj>!rr4?w6=V^Sa^z',
    'db': 'sentinoodlev2',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': True
}

CONNECTION_NAME = 'sentinoodle:europe-west2:sentinoodle-events'

EATEN = "eaten"
DB = None


def __get_cursor():
    """
    Helper function to get a cursor.
    PyMySQL does NOT automatically reconnect, so we must reconnect explicitly using ping().
    """
    try:
        return DB.cursor()
    except OperationalError:
        DB.ping(reconnect=True)
        return DB.cursor()


def ensure_db_connection():
    global DB

    if not DB:
        try:
            DB = pymysql.connect(**pymysql_config)
        except OperationalError:
            # If production settings fail, use local development ones
            pymysql_config['unix_socket'] = f'/cloudsql/{CONNECTION_NAME}'
            DB = pymysql.connect(**pymysql_config)


def get_session(device_id):
    fetch_session_query = f"""
        SELECT *
        FROM session
        WHERE device_id = '{device_id}'
        ORDER BY datetime_started DESC
        LIMIT 1;
    """
    ensure_db_connection()

    with __get_cursor() as cursor:
        cursor.execute(fetch_session_query)

    session_record = cursor.fetchone()
    return session_record


def insert_event(session_id, event_id, event_time):
    insert_event_query = f"""
        INSERT INTO event (id, session_id, event_name, published_at)
        VALUES (
            '{event_id}',
            {session_id},
            '{EATEN}',
            STR_TO_DATE('{event_time[:-1]}000', '%Y-%m-%dT%H:%i:%s.%f'));
    """
    ensure_db_connection()

    with __get_cursor() as cursor:
        cursor.execute(insert_event_query)


def decide_meal():
    pass


def handle_eaten(event, context):
    device_id = event["attributes"]["device_id"]
    session = get_session(device_id)
    session_id = session["id"]

    event_id = context.event_id
    event_time = event["attributes"]["published_at"]
    insert_event(session_id, event_id, event_time)
    print("Eaten!")

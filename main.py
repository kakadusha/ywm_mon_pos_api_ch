import os
import requests
import time
import logging
import pymysql
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv('API_URL')
HEADERS = {'Authorization': os.getenv('API_HEADERS')}
DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'port': 3306,
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'db': os.getenv('DB_DB')
}

GEO = 11079 # 225, 1, 10174, 11079 
DEVICE = 'ALL' # ALL, DESKTOP, MOBILE_AND_TABLET, MOBILE, TABLET
SLEEP_TIME_API = 0.5
MAX_ATTEMPTS = 5
SLEEP_TIME_ERR = 60


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info(f'{time.ctime()} - Старт работы скрипта.')
start_time = time.time()

connection = pymysql.connect(**DB_PARAMS)
cursor = connection.cursor()

create_table_query = '''
CREATE TABLE IF NOT EXISTS aggr (
    URL VARCHAR(500),
    QUERY VARCHAR(500),
    DATE DATE,
    DEMAND FLOAT,
    IMPRESSIONS FLOAT,
    CLICKS FLOAT,
    CTR FLOAT,
    POSITION FLOAT,
    UNIQUE INDEX query_date_uindex (QUERY, DATE)
);
'''
cursor.execute(create_table_query)

def api_request(url, headers, body):
    with requests.Session() as session:
        attempts = 0
        while attempts < MAX_ATTEMPTS:
            try:
                response = session.post(url, headers=headers, json=body)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as http_err:
                logging.error(f'{time.ctime()} - Ответ API: {http_err}. Повторная попытка через {SLEEP_TIME_ERR} секунд...')
                time.sleep(SLEEP_TIME_ERR)
                attempts += 1
        logging.error(f'{time.ctime()} - Превышено количество попыток, завершение скрипта.')

urls = []
while True:
    BODY_URL = {
        'offset': 0,
        'limit': 500,
        'device_type_indicator': DEVICE,
        'text_indicator': 'URL',
        'region_ids': [GEO],
        'filters': {
            'text_filters': [
                {'text_indicator': 'URL', 'operation': 'TEXT_CONTAINS', 'value': ''}
            ]
        }
    }
    response = api_request(API_URL, HEADERS, BODY_URL)
    for item in response['text_indicator_to_statistics']:
        urls.append(item['text_indicator']['value'])
    if not response['text_indicator_to_statistics'] or response['count'] <= 500:
        break
    BODY_URL['offset'] += 500
    time.sleep(SLEEP_TIME_API)

for url_value in urls:
    while True:
        BODY_QUERY = {
            'offset': 0,
            'limit': 500,
            'device_type_indicator': DEVICE,
            'text_indicator': 'QUERY',
            'region_ids': [GEO],
            'filters': {
                'text_filters': [
                    {'text_indicator': 'URL', 'operation': 'TEXT_MATCH', 'value': url_value}
                ]
            }
        }
        response = api_request(API_URL, HEADERS, BODY_QUERY)
        if not response['text_indicator_to_statistics'] or response['count'] <= 500:
            break
        BODY_QUERY['offset'] += 500
        time.sleep(SLEEP_TIME_API)

    data = []
    for text_indicator_to_statistics in response['text_indicator_to_statistics']:
        query_value = text_indicator_to_statistics['text_indicator']['value']
        for stat in text_indicator_to_statistics['statistics']:
            date = stat['date']
            field = stat['field']
            value = stat['value']
            row = {
                'URL': url_value,
                'QUERY': query_value,
                'DATE': date,
                'DEMAND': 0.0,
                'IMPRESSIONS': 0.0,
                'CLICKS': 0.0,
                'CTR': 0.0,
                'POSITION': 0.0
            }
            row[field] = value
            data.append(row)

    df = pd.DataFrame(data)
    df = df.groupby(['URL', 'QUERY', 'DATE'], as_index=False).sum()
    df.loc[df['IMPRESSIONS'] == 0.0, ['POSITION']] = None
    df = df[df['DEMAND'] != 0.0]
    df['DATE'] = pd.to_datetime(df['DATE']).dt.strftime('%d.%m.%Y')

    attempts = 0
    while attempts < MAX_ATTEMPTS:
        try:
            with connection.cursor() as cursor:
                for index, row in df.iterrows():
                    query = '''
                    INSERT IGNORE INTO aggr (URL, QUERY, DATE, DEMAND, IMPRESSIONS, CLICKS, CTR, POSITION)
                    VALUES (%s, %s, STR_TO_DATE(%s, '%%d.%%m.%%Y'), %s, %s, %s, %s, %s);
                    '''
                    values = (row['URL'], row['QUERY'], row['DATE'], row['DEMAND'], 
                            row['IMPRESSIONS'], row['CLICKS'], row['CTR'], 
                            row['POSITION'] if pd.notnull(row['POSITION']) else None)
                    cursor.execute(query, values)
                connection.commit()
            break    
        except Exception as err:
            logging.error(f'{time.ctime()} - Ответ базы данных: {err}. Повторная попытка через {SLEEP_TIME_ERR} секунд...')
            time.sleep(SLEEP_TIME_ERR)
            attempts += 1
    if attempts == MAX_ATTEMPTS:
        logging.error(f'{time.ctime()} - Превышено количество попыток, завершение скрипта.')

connection.close()
end_time = time.time()
logging.info(f'{time.ctime()} - Завершение работы скрипта. Выполнен за {end_time - start_time} секунд.')
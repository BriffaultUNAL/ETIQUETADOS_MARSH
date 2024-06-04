import logging
import sys
import os
from urllib.parse import quote
import sqlalchemy as sa
from sqlalchemy import text, Engine, Connection, Table, DATE
import yaml
import pandas as pd
from pandas import DataFrame
from telegram_bot import enviar_mensaje
from pandas.io.sql import SQLTable
import asyncio

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

yml_credentials_dir = os.path.join(act_dir, '..', 'config', 'credentials.yml')
yml_query_dir = os.path.join(proyect_dir, 'sql', 'querys.yml')

sql_query_1 = os.path.join(proyect_dir, 'sql', 'df_extend_inb_172.25.7.6.sql')
sql_query_2 = os.path.join(proyect_dir, 'sql', 'df_extend_inb_172.70.7.41.sql')
sql_query_3 = os.path.join(proyect_dir, 'sql', 'df_extend_out_172.25.7.6.sql')
sql_query_4 = os.path.join(proyect_dir, 'sql', 'df_extend_out_172.70.7.41.sql')
sql_query_5 = os.path.join(proyect_dir, 'sql', 'df_headcount.sql')
sql_query_6 = os.path.join(proyect_dir, 'sql', 'df_recording_log.sql')

logging.basicConfig(
    level=logging.INFO,
    filename=(os.path.join(act_dir, '..', 'log', 'logs_main.log')),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)


def get_engine(username: str, password: str, host: str, database: str, port: str = 3306, *_) -> Engine:
    return sa.create_engine(f"mysql+pymysql://{username}:{quote(password)}@{host}:{port}/{database}")


with open(yml_credentials_dir, 'r') as f:

    try:
        config = yaml.safe_load(f)
        source1, source2, source3 = config['source1'], config['source2'], config['source3']
        source4, source5 = config['source4'], config['source5']
    except yaml.YAMLError as e:
        logging.error(str(e), exc_info=True)


def engine_1() -> Connection:
    return get_engine(**source1).connect()


def engine_2() -> Connection:
    return get_engine(**source2).connect()


def engine_3() -> Connection:
    return get_engine(**source3).connect()


def engine_4() -> Connection:
    return get_engine(**source4).connect()


def engine_5() -> Connection:
    return get_engine(**source5).connect()


def import_query(sql):

    with open(sql, 'r') as f_2:

        try:
            querys = f_2.read()
            query = text(querys)
            return query

        except yaml.YAMLError as e_2:
            logging.error(str(e_2), exc_info=True)


def to_sql_replace(table: SQLTable, con: Engine | Connection, keys: list[str], data_iter):

    satable: Table = table.table
    ckeys = list(map(lambda s: s.replace(' ', '_'), keys))
    data = [dict(zip(ckeys, row)) for row in data_iter]
    values = ', '.join(f':{nm}' for nm in ckeys)
    # news = ', '.join(f'{nm} = new.{nm}' for nm in keys)
    # stmt = f"INSERT INTO {satable.name} VALUES ({values}) AS new ON DUPLICATE KEY UPDATE {news}"
    stmt = f"REPLACE INTO {satable.name} VALUES ({values})"

    con.execute(text(stmt), data)


def extract_to_sql(query, engine):
    with engine as con:
        df = pd.read_sql_query(query, con)
        return df


df_headcount = extract_to_sql(import_query(sql_query_5), engine_2())
df_recording_log_41 = extract_to_sql(import_query(sql_query_6), engine_3())
df_recording_log_6 = extract_to_sql(import_query(sql_query_6), engine_5())


def transform(query_extends: str, df_recording: pd.DataFrame, tmo: int, gestion: str) -> DataFrame:

    df_extend = extract_to_sql(import_query(query_extends), engine_1())

    df_join_exthead = pd.DataFrame()

    df_join_exthead = pd.merge(
        df_extend, df_headcount, left_on='user', right_on='Documento', how='left')

    df_join_exthead['aliado'] = 'COS-BOGOTA'

    df_join_exthead['proceso'] = 'MARSH'

    df_join_exthead['fecha'] = pd.to_datetime(df_join_exthead['call_date'])

    df_join_exthead['day'] = (df_join_exthead['fecha'].dt.day.astype(
        str)).apply(lambda x: '0'+x if 2 > len(x) else x)

    df_join_exthead['month'] = df_join_exthead['fecha'].dt.month.astype(str)

    df_join_exthead['year'] = df_join_exthead['fecha'].dt.year.astype(str)

    df_join_exthead['date'] = df_join_exthead['day']+'-' + \
        df_join_exthead['month']+'-'+df_join_exthead['year']

    df_join_exthead['hora'] = df_join_exthead['call_date'].astype(
        str).apply(lambda x: x[-8:].replace(':', '-'))

    df_join_exthead = df_join_exthead.drop(['day', 'month', 'year'], axis=1)

    df_join_exthead = df_join_exthead.rename(columns={'length_in_sec': 'TMO',
                                                      'status_name': 'gestion'})

    df_join_exthead['gestion'] = df_join_exthead['gestion'].apply(
        lambda x: str(x).replace(' ', '_'))

    df_join_exthead['fecha'] = pd.to_datetime(
        df_join_exthead['call_date']).dt.strftime('%Y-%m-%d')

    df_recording['lead_id'] = df_recording['lead_id'].astype(str)

    df_recording['fecha'] = pd.to_datetime(
        df_recording['start_time']).dt.strftime('%Y-%m-%d')

    df_join_exthead = pd.merge(
        df_join_exthead, df_recording, on=['fecha', 'lead_id'], how='left')

    df_join_exthead = df_join_exthead.dropna(subset='TMO')

    df_join_exthead['TMO'] = df_join_exthead['TMO'].astype(float).astype(int)

    df_join_exthead = df_join_exthead.query(f'TMO >= {tmo}')

    df_join_exthead['llave'] = df_join_exthead['call_date'].astype(
        str)+'-'+df_join_exthead['phone_number_dialed']

    df_join_exthead = df_join_exthead.drop_duplicates(
        subset='llave').drop('llave', axis=1)

    df_join_exthead = df_join_exthead.drop_duplicates(subset='location')

    df_join_exthead['tipo_gestion'] = gestion

    return df_join_exthead


def load(df: pd.DataFrame):

    asyncio.run(enviar_mensaje(f'cargados {len(df)} datos'))
    print(f'cargados {len(df)} datos')

    with engine_4() as con:

        try:

            df.to_sql('etiquetados_marsh', con=con, if_exists='append',
                      index=False, dtype={'DATE': DATE}, method=to_sql_replace)

            logging.info(f'Se cargan {len(df)} datos')

        except KeyError as e:

            logging.error(str(e), exc_info=True)

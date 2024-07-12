import pandas as pd
from datetime import datetime
from app.ExtractFiles import ExtractFiles
from app.TransformFiles import TransformFiles
from app.LoadFiles import LoadFiles
import json
import glob
import warnings
from dotenv import load_dotenv
import os
warnings.filterwarnings("ignore")

load_dotenv(dotenv_path='./env', override=True)

POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

# diretório onde os arquivos estão localizados
zip_file_path = 'data.zip'
extract_dir = 'data'

#Extraindo arquivos
extract_files = ExtractFiles(zip_file_path=zip_file_path, extract_dir=extract_dir)

extract_files.zip_extract()
                                                        
arquivos = glob.glob('.//data/*csv')

# Imprima os nomes dos arquivos
files = pd.DataFrame(arquivos, columns=['arquivos'])
files['arquivos'] = files['arquivos'].apply(lambda x: x.split('\\')[1])
files['arquivos'] = files['arquivos'].apply(lambda x: x.split('.')[0])
files['date'] = pd.to_datetime(files['arquivos'], format='%Y%m%d-%H%M%S%f')
files = files.sort_values(by='date', ascending=True)
#files.tail(3)

for index, file in files.iterrows():
    print(f'INICIANDO A CARGA DOS DADOS: {index} / {file.shape[1]}')
    transform_files = TransformFiles(files_name=f"./data/{file['arquivos']}.csv")
    order = transform_files.order()
    order_temp = order.drop(columns=['op'])

    load_files = LoadFiles(schema='public', table='order_temp')
    load_files.to_table(order)

    def insert_dimensao(schema, table, key):
        try:
            load_files = LoadFiles(schema=schema, table=table)
            load_files.execute_query(query=f'''
            insert into {table} ({table})
            select distinct ot.{key} as {table}
            from order_temp ot
            left join {table} on2 on ot.{key} = on2.{table}
            where on2.{table} is null
            order by 1''')
            print(f'Tabela Dimensão {table} carregada com sucesso!')
        except Exception as e:
            print(f'Erro: {e}')

    insert_dimensao(schema='public', table='order_number', key='oid_id')
    insert_dimensao(schema='public', table='order_tracking_code', key='order_tracking_code')
    insert_dimensao(schema='public', table='order_status', key='order_status')
    insert_dimensao(schema='public', table='order_from', key='order_from')
    insert_dimensao(schema='public', table='order_to', key='order_to')

    order_insert = order[order['op'] == 'I'].drop(columns=['op'])

    load_files = LoadFiles(schema='public', table='order_temp_insert')
    load_files.to_table(order_insert)

    load_files = LoadFiles(schema='public', table='order_main')

    try:
        query = '''insert into order_main (oid_id, created_at, updated_at, last_sync_tracker, order_created_at, order_tracking_code_id, order_status_id, order_description, order_tracker_type, order_from_id, order_to_id)
        select on2.id as oid_id,
        created_at,
        updated_at,
        last_sync_tracker,
        order_created_at,
        otc.id as order_tracking_code_id,
        os.id as order_status,
        order_description,
        order_tracker_type,
        of2.id as order_from_id,
        ot2.id as order_to_id
        from order_temp_insert ot 
        left join order_number on2 on on2.order_number  = ot.oid_id 
        left join order_tracking_code otc on otc.order_tracking_code = ot.order_tracking_code
        left join order_status os on os.order_status = ot.order_status 
        left join order_from of2 on of2.order_from = ot.order_from
        left join order_to ot2 on ot2.order_to = ot.order_to'''
        load_files.execute_query(query)
        print(f'Tabela Dimensão order_main carregada com sucesso!') 
    except Exception as e:
        print(e)

    order_update = order[order['op'] == 'U'].drop(columns=['op'])

    load_files = LoadFiles(schema='public', table='order_temp_update')
    load_files.to_table(order_update)
#Apaga os registros que serão atualizados
    try:
        query = '''with delete_ordens as
                    (select distinct on2.id
                    from order_temp_update om 
                    left join order_number on2 on om.oid_id = on2.order_number
                    where on2.order_number is not null)
                    DELETE FROM public.order_main ord
                    WHERE ord.oid_id in (select oid_id from delete_ordens)'''
        load_files.execute_query(query)
        print(f'Tabela Dimensão order_main atualizada com sucesso!')
    except Exception as e:
        print(e)

#Insere os registros sem conflito
    try:
        query = '''insert into order_main (oid_id, created_at, updated_at, last_sync_tracker, order_created_at, order_tracking_code_id, order_status_id, order_description, order_tracker_type, order_from_id, order_to_id)
        select on2.id as oid_id,
        created_at,
        updated_at,
        last_sync_tracker,
        order_created_at,
        otc.id as order_tracking_code_id,
        os.id as order_status,
        order_description,
        order_tracker_type,
        of2.id as order_from_id,
        ot2.id as order_to_id
        from order_temp_update ot 
        left join order_number on2 on on2.order_number  = ot.oid_id 
        left join order_tracking_code otc on otc.order_tracking_code = ot.order_tracking_code
        left join order_status os on os.order_status = ot.order_status 
        left join order_from of2 on of2.order_from = ot.order_from
        left join order_to ot2 on ot2.order_to = ot.order_to'''
        load_files.execute_query(query)
        print(f'Tabela Dimensão order_main atualizada com sucesso!')
    except Exception as e:
        print(e)

#Apaga as tabelas temporarias
try:
    tables = ['order_temp', 'order_temp_insert', 'order_temp_update']
    for table in tables:
        load_files.execute_query(f'drop table {table}')
        print(f'Tabela Dimensão order_main atualizada com sucesso!')
except Exception as e:
    print(e)
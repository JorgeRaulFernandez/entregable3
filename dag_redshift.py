from datetime import datetime, timedelta
from email.policy import default
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import requests
import urllib.parse
!pip install psycopg2-binary
import os
import psycopg2

dag_path = os.getcwd()

def get_data(exec_date):
    try:
        print(f"Adquiriendo data para la  fecha: {exec_date}")
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')
        file_date_path = f"{date.strftime('%Y-%m-%d')}/{date.hour}"
        def get_api_call(ids,**kwargs):
            API_BASE_URL = "https://apis.datos.gob.ar/series/api/"
            kwargs["ids"]=",".join(ids)
            return "{}{}?{}".format(API_BASE_URL,"series",urllib.parse.urlencode(kwargs))
        api_call_ventas_totales = get_api_call(["458.1_TOTALTAL_ABRI_M_5_38"],start_date="2019-01-01")

        result_ventas_totales = requests.get(api_call_ventas_totales).json()

        api_call_ventas_comida = get_api_call(["458.1_PATIO_COMIDAS_ABRI_M_13_59"],start_date="2019-01-01")

        result_ventas_comida = requests.get(api_call_ventas_comida).json()

        api_call_ventas_jugueterias = get_api_call(["458.1_JUGUETERIARIA_ABRI_M_10_48"],start_date="2019-01-01")
 
        result_ventas_jugueterias = requests.get(api_call_ventas_jugueterias).json()

        api_call_ventas_farmacias = get_api_call(["458.1_PERFUMERIACIA_ABRI_M_19_16"],start_date="2019-01-01")

        result_ventas_farmacias = requests.get(api_call_ventas_farmacias).json()

        api_call_ventas_diversion = get_api_call(["458.1_DIVERSION_NTO_ABRI_M_23_37"],start_date="2019-01-01")

        result_ventas_diversion = requests.get(api_call_ventas_diversion).json()

        api_call_ventas_libreria = get_api_call(["458.1_LIBRERIA_PRIA_ABRI_M_18_18"],start_date="2019-01-01")

        result_ventas_libreria = requests.get(api_call_ventas_libreria).json()

        
    except ValueError as e:
        print(e)
    raise e

def transform_data(exec_date):
    print(f"Transformando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    file_date_path = f"{date.strftime('%Y-%m-%d')}/{date.hour}"
    df1 = pd.DataFrame(result_ventas_totales['data'],columns=['date_from','ventas_totales'])
    df2 = pd.DataFrame(result_ventas_comida['data'],columns=['date_from','ventas_comida'])
    df3 = pd.DataFrame(result_ventas_jugueterias['data'],columns=['date_from','ventas_jugueterias'])
    df4 = pd.DataFrame(result_ventas_farmacias['data'],columns=['date_from','ventas_farmacias'])
    df5 = pd.DataFrame(result_ventas_diversion['data'],columns=['date_from','ventas_diversion'])
    df6 = pd.DataFrame(result_ventas_libreria['data'],columns=['date_from','ventas_libreria'])
    df = pd.merge(df1, df2, on='date_from', how='outer')
    df = pd.merge(df, df3, on='date_from', how='outer')
    df = pd.merge(df, df4, on='date_from', how='outer')
    df = pd.merge(df, df5, on='date_from', how='outer')
    df = pd.merge(df, df6, on='date_from', how='outer')
    df['frequency'] = 'monthly'
    df_with_date_label_data = df.copy()
    df_with_date_label_data['date_from'] = pd.to_datetime(df['date_from'])
    df_with_date_label_data['month_year'] = df_with_date_label_data['date_from'].dt.strftime("%b-%y")
    df_with_date_label_data['iso_week_year'] = df_with_date_label_data['date_from'].dt.year.astype(str) + '-' + df_with_date_label_data['date_from'].dt.isocalendar().week.apply(lambda x: f'W{x:02d}')
    df_con_duplicados = pd.concat([df_with_date_label_data, df_with_date_label_data.head(3)], ignore_index=True)
    df_to_write =df_con_duplicados.drop_duplicates()
    df_to_write.isnull().sum()
    df_to_write.fillna(0, inplace=True)
    df_to_write.isnull().sum()


def load_data(exec_date):
    print(f"Cargando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    file_date_path = f"{date.strftime('%Y-%m-%d')}/{date.hour}"
    env= os.environ
    conn = psycopg2.connect(
        host=env['AWS_REDSHIFT_HOST'],
        port=env['AWS_REDSHIFT_PORT'],
        dbname=env['AWS_REDSHIFT_DBNAME'],
        user=env['AWS_REDSHIFT_USER'],
        password=env['AWS_REDSHIFT_PASSWORD']
    )
    cursor = conn.cursor()
    cursor.execute(f"""
    create table if not exists {env['AWS_REDSHIFT_SCHEMA']}.ventas_centros_compras_entregable2 (
        date_from TIMESTAMP distkey,
        ventas_totales_pesos decimal(15,2),
        ventas_comida decimal(15,2),
        ventas_jugueterias decimal(15,2),
        ventas_farmacias decimal(15,2),
        ventas_diversion decimal(15,2),
        ventas_liberia decimal(15,2),
        frequency varchar(12),
        month_year varchar(12),
        iso_week_year varchar(12)
    ) sortkey(date_from);
    """)
    conn.commit()
    cursor.close()
    print("Table created!")
    cursor = conn.cursor()
    cursor.execute(f"""
    SELECT
        distinct tablename
    FROM
        PG_TABLE_DEF
    WHERE
        schemaname='{env['AWS_REDSHIFT_SCHEMA']}';
    """)
    resultado = cursor.fetchall()
    print(", ".join(map(lambda x: x[0],resultado)))
    cursor.close()
    values = ','.join(['%s'] * len(df_to_write.columns))
    query = f"INSERT INTO ventas_centros_compras_entregable2 VALUES ({values})"
    data = [tuple(row) for row in df_to_write.values]
    cursor = conn.cursor()
    cursor.executemany(query, data)
    conn.commit()
    cursor = conn.cursor()
    query = "SELECT * FROM ventas_centros_compras_entregable2"
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    df_resultado = pd.DataFrame(results, columns=columns)
    cursor.close()
    conn.close()

default_args={
    'owner': 'Jorge',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='dag_con_conexion_postgres',
    description= 'Entregable3',
    start_date=datetime(2023,7,10),
    schedule_interval='0 3 * * 0#1' 
    
    ##Lo configuro para que se actualice el primer domingo de cada mes a las 3 de la mañana, dado que los datos de la api se refrescan una vez al mes
    
    ) as dag:
        task_1 = PythonOperator(
            task_id='get_data',
            python_callable=get_data,
            op_args=["{{ ds }} {{ execution_date.hour }}"],
        )

        task_2 = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data,
            op_args=["{{ ds }} {{ execution_date.hour }}"],
        )
        task_3 = PythonOperator(
            task_id='load_data',
            python_callable=load_data,
            op_args=["{{ ds }} {{ execution_date.hour }}"],
        )

task_1 >> task_2 >> task_3

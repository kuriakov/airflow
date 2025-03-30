# Импортируем необходимые библиотеки
import requests as req  # для выполнения HTTP-запросов
import pandas as pd  # для обработки данных
from datetime import datetime, timedelta  # для работы с датами
import json  # для парсинга json
from clickhouse_driver import Client  # для подключения к ClickHouse
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 

# Настройка подключения к базе данных ClickHouse

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
import logging

# обращение к CONNECTION  exchange_rate
# url api в Connections https://api.exchangerate.host/timeframe
HOST_EXCR = BaseHook.get_connection("exchange_rate").host # TODO: перенести внутрь функции
PASSWORD_EXCR = BaseHook.get_connection("exchange_rate").password
# чтобы десериализовать JSON (получить просто dict) используется параметр deserialize_json=True
# В Variables для exchange_rate лежит: {"s_file": "exchangerate_data.json", "csv_file": "exchangerate_data.csv"}

exchange_rate = Variable.get("exchange_rate", deserialize_json=True)

# Функция для извлечения данных с API Центрального банка и сохранения их в локальный файл
def extract_data(url, s_file, **kwargs):
    """
    Эта функция получает на вход готовый url для запроса данных за нужную дату из API
    и выгружает данные по валютам, используя GET-запрос,
    и сохраняет результат в локальный файл `s_file`.
    """
    request = req.get(url)
    
    log = logging.getLogger("task log")
    log.info(f"HTTP ответ по запросу отчета: {request}")
    # Сохраняем полученные данные (в формате XML) в локальный файл
    with open(s_file, 'w', encoding='utf-8') as tmp_file:
        tmp_file.write(request.text)  # Записываем текст ответа в файл

# Функция для обработки данных в формате JSON и преобразования их в CSV
def transform_data(s_file, csv_file, **kwargs):
    """
    Эта функция обрабатывает полученные данные в формате JSON
    и преобразует их в табличном формате для дальнейшей работы.
    В конце данные записываются в CSV файл
    """
    date = kwargs.get("date")
    with open(s_file, "r") as f:
        data = json.load(f)
    # Парсим курсы валют из quotes
    df = pd.DataFrame.from_dict(data["quotes"], orient="index").stack().reset_index()
    df.columns = ["date", "currency_pair", "value"]
    # Разбиваем валютную пару на 2 столбца
    df[["currency_source", "currency"]] = df["currency_pair"].str.extract(r"^(USD)([A-Z]+)$")
    # тут беру date из контекста, хотя это поле есть в данных уже
    #df["date"] = pd.to_datetime(df["date"])
    df["date"] = pd.to_datetime(date)
    # Убираем ненужный столбец
    df.drop(columns=["currency_pair"], inplace=True)

    df.to_csv(csv_file, index=False)

# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(csv_file, table_name, **kwargs):
    """
    Эта функция считывает CSV файл, создает таблицу в
    базе данных ClickHouse и добавляет данные в неё
    """
    df = pd.read_csv(csv_file)

    # Создание таблицы, ЕСЛИ НЕ СУЩЕСТВУЕТ ТО СОЗДАТЬ ТАБЛИЦУ
    #client.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (date String, currency_source String, currency String, value Float64) ENGINE Log')

    # Запись data frame в ClickHouse
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    ch_hook.execute(f'INSERT INTO {table_name} VALUES', df.to_dict('records'))

    # TODO: изменить движок таблицы для идемпотентности, сейчас вставляет дубли

# Функция для обработки ошибки и отправки сообщения
def on_failure_callback(context):
    dag_id = context["dag"].dag_id  # Получаем ID DAG-а
    task_id = context["task_instance"].task_id  # Получаем ID задачи
    execution_date = context["execution_date"]  # Дата выполнения DAG-а
    exception = context["exception"]  # Ошибка
    message = (
        f"🚨 *Airflow Alert! Some shit has happened!*\n"
        f"💀 DAG: `{dag_id}`\n"
        f"💣 Task: `{task_id}`\n"
        f"📅 Execution Date: `{execution_date}`"
    )
    send_message = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_154076484',
        chat_id='-1002503497257',
        text=message,
        dag=dag)
    return send_message.execute(context=context)

def on_success_callback(context):
    dag_id = context["dag"].dag_id  # Получаем ID DAG-а
    execution_date = context["execution_date"]  # Дата выполнения DAG-а
    message = (
        f"🟢*DAG RUN `{dag_id}` FOR `{execution_date}` COMPLETED SUCCESSFULLY*\n"
    )
    send_message = TelegramOperator(
        task_id='send_sucess_message_telegram',
        telegram_conn_id='telegram_154076484',
        chat_id='-1002503497257',
        text=message,
        dag=dag)
    return send_message.execute(context=context)

default_args = {
    #'on_success_callback': on_success_callback,  # сделал только для последней таски
    'on_failure_callback': on_failure_callback, # Задача упала
    #'on_retry_callback': another_function, # Задача повторяется
}

with DAG(
    '154076484_ikuryakov_task_71',
    schedule_interval='@daily',  # Запуск каждый день
    start_date=datetime(2024,1,1),
    end_date=datetime(2024,1,5),
    max_active_runs=1,
    default_args=default_args,
) as dag:

    # Операторы для выполнения шагов
    extract_task = PythonOperator(
                                 task_id='extract_data',
                                 python_callable=extract_data,
                                 # Передача аргументов через словарь, а не список
                                 op_kwargs = {
                                             'url': f'{HOST_EXCR}?access_key={PASSWORD_EXCR}' + '&start_date={{ ds }}&end_date={{ ds }}&source=USD',
                                             's_file': exchange_rate['s_file'], # название файла из переменной
                                             'date': '{{ ds }}',
                                             }
                                 )

    transform_task = PythonOperator(
                                 task_id='transform_data',
                                 python_callable=transform_data,
                                 # Передача аргументов через словарь, а не список
                                 op_kwargs = {
                                             's_file': exchange_rate['s_file'],
                                             'csv_file': exchange_rate['csv_file'],
                                             'date': '{{ ds }}',
                                             }
                                 )
    
 # Оператор для выполнения запроса
    create_table = ClickHouseOperator(
                                     task_id='create_table',
                                     sql="""CREATE TABLE IF NOT EXISTS {{ params.table_name }} 
                                     (date String, currency_source String, currency String, value Float64) ENGINE Log""",
                                     clickhouse_conn_id='clickhouse_default', 
                                     params={'table_name': '154076484_ikuryakov_assign_71'},
                                     dag=dag,
                                    )

    upload_task = PythonOperator(
                                 task_id='load_data',
                                 python_callable=upload_to_clickhouse,
                                 # Передача аргументов через словарь, а не список
                                 op_kwargs = {
                                             'csv_file': exchange_rate['csv_file'],
                                             'table_name':'154076484_ikuryakov_assign_71',
                                             #'client': CH_CLIENT # теперь вместо него хук
                                             }
                                 )
    
    # Оператор для выполнения запроса
    create_new_order_table = ClickHouseOperator(
        task_id='create_new_order_table',
        sql='CREATE TABLE IF NOT EXISTS sandbox.ikuryakov_new_order_table (date String, order_id Int64, purchase_rub Float64, purchase_usd Float64) ENGINE Log',
        clickhouse_conn_id='clickhouse_default', 
        dag=dag,
    )
    
 # Оператор для выполнения запроса
 # Не забывайте использовать  {{ ds }} а также если вам нужно обратиться к другой базу данных
 # То просто пропишите эт ов SQL <airflow.order>
    insert_join = ClickHouseOperator(
        task_id='insert_join',
        #TODO: оптимизировать запрос
        sql="""INSERT INTO sandbox.ikuryakov_new_order_table
            select 
                  o.date,
                  order_id,
                  purchase_rub,
                  purchase_rub / value as purchase_usd
            from airflow.orders as o
            any left join sandbox.154076484_ikuryakov_assign_71 as r
            on (r.currency='RUB'
            and o.date = r.date)
            where o.date='{{ ds }}'
            """,
        clickhouse_conn_id='clickhouse_default', 
        dag=dag,
        on_success_callback=on_success_callback 
    )

    # Определение зависимостей между задачами
    extract_task >> transform_task >> [create_table, create_new_order_table] >> upload_task >> insert_join
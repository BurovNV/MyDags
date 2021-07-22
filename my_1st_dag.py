# импорт необходимых бибиотек
from airflow import DAG
from airflow.operators import PythonOperator as PO
from datetime import datetime
import pathlib

# настройки по умолчанию
default_args = {
  'owner':           'BurovNV',              # владелец дэга
  'depends_on_past': False,                  # зависим ли запуск от прошлого трая вчера 
  'start_date':      datetime(2021, 1, 1),   # дата начала исполнения
  'retries':         0                       # число попыток перезапуска при фэйле в один день
}

# функция для создания графа
dag = DAG('python_hello_world_dag',          # название
          default_args=default_args,         # передача    словаря аргументов
          catchup=False,                     #
          schedule_interval='00 10 **')      # расписание

# создаем функции которые позже будем вызывать в дэге
# но создавать их можно и перед самим телом таски
def hello():
  return print('Hello world!')

# пишем таски
task_1 = PO(                                 # 
  task_id='print_hello_world',               # название
  python_callable=hello,                     # какую функцию вызываем
  dag=dag)                                   # вызов функции создания графа

# еще одна функция для таски
def summator():
  return print(f'сумма 1 + 2 = {1+2}')

# таска с сумматором
task_2 = PO(
    task_id='print_sum_1_and_2',             # название
    python_callable=summator,                # какую функцию вызываем
    dag=dag)                                 # вызов функции создания графа

# завмсимость таск друг от друга
task_1 >> task_2

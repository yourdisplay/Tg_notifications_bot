import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
#from read_db.CH import Getch
import pandahouse as ph

import matplotlib.dates as mdates
from datetime import datetime, timedelta

from airflow.decorators import dag, task


default_args = {
    'owner': 'ri-g',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 3)
    }

schedule_interval = '0 11 * * *'

connection = {'host': '****',
              'password': '***',
              'user': '***',
              'database': '***'
                }

token = '***'
chat_id = '***'
bot = telegram.Bot(token=token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_task_7_1():
    
    @task()
    def extract_1():
        query_metrics = """
        SELECT toDate(time) AS date, count(DISTINCT user_id) AS dau, sum(action = 'view') as views, sum(action = 'like') as likes, ROUND(likes/views, 2) as ctr
        FROM {db}.feed_actions
        WHERE date = today() - 1
        GROUP BY date
        """

        df_metrics = ph.read_clickhouse(query_metrics, connection=connection)
        return df_metrics

    @task()
    def extract_2():
        query_within_7 = """
        SELECT toDate(time) AS date, count(DISTINCT user_id) AS dau, sum(action = 'view') as views, sum(action = 'like') as likes, likes/views as ctr
        FROM {db}.feed_actions
        WHERE date >= today() - 7 and date <= today() - 1 
        GROUP BY date
        ORDER BY date
        """
        
        df_within_7 = ph.read_clickhouse(query_within_7, connection=connection)
        return df_within_7
    
    @task()
    def send_metrics(df_metrics):
        msg = "Основные метрики продукта по состоянию на {0}:\n * Уникальные пользователи (DAU): {1}\n * Просмотры: {2}\n * Лайки: {3}\n * Отношение лайки/просмотры (CTR): {4}".format(df_metrics['date'][0].strftime("%d-%m-%Y"), int(df_metrics['dau']), int(df_metrics['views']), int(df_metrics['likes']), float(df_metrics['ctr']))

        bot.send_message(chat_id=chat_id, text=msg)
        
                
    @task()
    def send_plot(df_within_7):
        plt.figure(figsize=(12,9))
        plt.suptitle('Динамика метрик за 7 дней')

        plt.subplot(221)
        plt.ylabel('Количество')
        plt.xlabel('Дата')
        plt.title('Уникальные пользователи (DAU)')
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())
        plt.plot(df_within_7.date, df_within_7.dau, 'o-', label='dau')
        plt.gcf().autofmt_xdate()
        plt.legend(loc="best")

        plt.subplot(222)
        plt.ylabel('Количество')
        plt.xlabel('Дата')
        plt.title('Просмотры')
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())
        plt.plot(df_within_7.date, df_within_7.views, 'o-r', label='views')
        plt.gcf().autofmt_xdate()
        plt.legend(loc="best")

        plt.subplot(223)
        plt.ylabel('Количество')
        plt.xlabel('Дата')
        plt.title('Лайки')
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())
        plt.plot(df_within_7.date, df_within_7.ctr, 'o-g', label='likes')
        plt.gcf().autofmt_xdate()
        plt.legend(loc="best")

        plt.subplot(224)
        plt.ylabel('Значение CTR')
        plt.xlabel('Дата')
        plt.title('Отношение лайки/просмотры (CTR)')
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())
        plt.plot(df_within_7.date, df_within_7.ctr, 'o-', color='black', label='ctr')
        plt.gcf().autofmt_xdate()
        plt.legend(loc="best")

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.close

        bot.sendPhoto(chat_id=chat_id, photo=plot_object.getvalue())
            
    df_metrics = extract_1()
    df_within_7 = extract_2()
    send_metrics(df_metrics)
    send_plot(df_within_7)
    
dag_task_7_1 = dag_task_7_1()

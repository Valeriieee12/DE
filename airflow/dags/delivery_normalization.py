"""
DAG для нормализации данных доставок из Parquet в PostgreSQL
Автоматически разбивает денормализованные данные на нормализованные таблицы
"""
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'delivery_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_normalize(**context):
    """Извлекает данные из Parquet и нормализует их"""
    
    # Путь к файлу данных
    file_path = '/opt/airflow/data/deliveries.parquet'
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл {file_path} не найден! Положите deliveries.parquet в airflow/data/")
    
    # Читаем Parquet файл
    print(f"Чтение файла: {file_path}")
    df = pd.read_parquet(file_path)
    print(f"Прочитано {len(df):,} строк, {len(df.columns)} колонок")
    
    # 1. Создаем таблицу пользователей
    users_df = df[['user_id', 'user_phone']].drop_duplicates()
    users_df = users_df.dropna(subset=['user_id'])
    print(f"Пользователей: {len(users_df):,}")
    
    # 2. Создаем таблицу магазинов
    stores_df = df[['store_id', 'store_address']].drop_duplicates()
    stores_df = stores_df.dropna(subset=['store_id'])
    print(f"Магазинов: {len(stores_df)}")
    
    # 3. Создаем таблицу курьеров
    drivers_df = df[['driver_id', 'driver_phone']].drop_duplicates()
    drivers_df = drivers_df.dropna(subset=['driver_id'])
    print(f"Курьеров: {len(drivers_df):,}")
    
    # 4. Создаем таблицу товаров
    items_df = df[['item_id', 'item_title', 'item_category']].drop_duplicates()
    items_df = items_df.dropna(subset=['item_id'])
    print(f"Товаров: {len(items_df)}")
    
    # 5. Создаем таблицу заказов
    orders_columns = [
        'order_id', 'user_id', 'store_id', 'driver_id', 
        'address_text', 'created_at', 'paid_at', 'delivery_started_at',
        'delivered_at', 'canceled_at', 'payment_type', 'order_discount',
        'order_cancellation_reason', 'delivery_cost'
    ]
    orders_df = df[orders_columns].drop_duplicates(subset=['order_id'])
    orders_df = orders_df.dropna(subset=['order_id', 'user_id', 'store_id', 'created_at'])
    print(f"Заказов: {len(orders_df):,}")
    
    # 6. Создаем таблицу товаров в заказах
    order_items_columns = [
        'order_id', 'item_id', 'item_quantity', 'item_price',
        'item_discount', 'item_canceled_quantity', 'item_replaced_id'
    ]
    order_items_df = df[order_items_columns]
    order_items_df = order_items_df.dropna(subset=['order_id', 'item_id'])
    print(f"Позиций в заказах: {len(order_items_df):,}")
    
    # Сохраняем информацию о данных
    context['ti'].xcom_push(key='dataframes_info', value={
        'users': len(users_df),
        'stores': len(stores_df),
        'drivers': len(drivers_df),
        'items': len(items_df),
        'orders': len(orders_df),
        'order_items': len(order_items_df)
    })
    
    # Возвращаем DataFrame для загрузки
    return {
        'users': users_df,
        'stores': stores_df,
        'drivers': drivers_df,
        'items': items_df,
        'orders': orders_df,
        'order_items': order_items_df
    }

def load_to_postgres(**context):
    """Загружает нормализованные данные в PostgreSQL"""
    
    # Получаем DataFrame из предыдущей задачи
    ti = context['ti']
    data_dict = ti.xcom_pull(task_ids='extract_normalize_data', key='return_value')
    
    if not data_dict:
        raise ValueError("Нет данных для загрузки!")
    
    # Подключаемся к PostgreSQL
    hook = PostgresHook(postgres_conn_id='main_postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    loaded_counts = {}
    
    try:
        for table_name, df in data_dict.items():
            if df is not None and not df.empty:
                # Очищаем таблицу перед загрузкой (идемпотентность)
                truncate_sql = f"TRUNCATE TABLE {table_name} CASCADE;"
                cursor.execute(truncate_sql)
                
                # Загружаем данные
                print(f"Загрузка {len(df):,} строк в таблицу {table_name}...")
                
                # Используем fast_executemany для быстрой загрузки
                placeholders = ', '.join(['%s'] * len(df.columns))
                columns = ', '.join(df.columns)
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                # Разбиваем на батчи
                batch_size = 10000
                rows = df.values.tolist()
                
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    cursor.executemany(sql, batch)
                    conn.commit()
                    
                    if i % 50000 == 0:
                        print(f"  Загружено {i:,} строк...")
                
                loaded_counts[table_name] = len(df)
                print(f"✓ Загружено {len(df):,} строк в {table_name}")
        
        print(f"\n✅ Всего загружено:")
        for table, count in loaded_counts.items():
            print(f"   {table}: {count:,} строк")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при загрузке: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

with DAG(
    'delivery_normalization',
    default_args=default_args,
    description='Нормализация данных доставок из Parquet в PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    tags=['delivery', 'etl', 'normalization'],
) as dag:

    create_tables = PostgresOperator(
        task_id='create_normalized_tables',
        postgres_conn_id='main_postgres',
        sql='''
        -- Таблицы будут созданы автоматически при инициализации БД
        -- из файла postgres/init_delivery_db.sql
        -- Этот оператор для дополнительной гарантии
        SELECT 'Таблицы уже созданы при инициализации БД' as status;
        '''
    )
    
    extract_normalize = PythonOperator(
        task_id='extract_normalize_data',
        python_callable=extract_and_normalize,
        provide_context=True,
    )
    
    load_data = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,
    )
    
    create_tables >> extract_normalize >> load_data

"""
DAG для построения витрин данных из нормализованных таблиц
Использует PySpark для обработки больших объемов данных
"""
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
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

def create_datamarts_tables(**context):
    """Создает таблицы для витрин если их нет"""
    # Эта функция будет выполняться как PythonOperator
    # Но таблицы уже созданы в init_delivery_db.sql
    print("Таблицы витрин dm_orders и dm_items уже созданы в БД")
    return True

with DAG(
    'delivery_datamarts',
    default_args=default_args,
    description='Построение витрин данных для аналитики доставок',
    schedule_interval='@daily',
    catchup=False,
    tags=['delivery', 'datamart', 'analytics'],
) as dag:

    # 1. Очищаем витрины перед обновлением (идемпотентность)
    truncate_datamarts = PostgresOperator(
        task_id='truncate_datamarts',
        postgres_conn_id='main_postgres',
        sql='''
        TRUNCATE TABLE dm_orders;
        TRUNCATE TABLE dm_items;
        '''
    )
    
    # 2. Построение витрины заказов (dm_orders)
    build_orders_datamart = PostgresOperator(
        task_id='build_orders_datamart',
        postgres_conn_id='main_postgres',
        sql='''
        -- ВИТРИНА ЗАКАЗОВ
        INSERT INTO dm_orders (
            report_date, report_year, report_month, report_day,
            city, store_id,
            unique_customers, total_orders, total_delivered_orders,
            total_canceled_orders, total_turnover, total_revenue,
            avg_order_value
        )
        WITH city_extracted AS (
            SELECT 
                store_id,
                CASE 
                    WHEN store_address LIKE '%Москва%' THEN 'Москва'
                    WHEN store_address LIKE '%Санкт-Петербург%' THEN 'Санкт-Петербург'
                    WHEN store_address LIKE '%Новосибирск%' THEN 'Новосибирск'
                    WHEN store_address LIKE '%Екатеринбург%' THEN 'Екатеринбург'
                    WHEN store_address LIKE '%Казань%' THEN 'Казань'
                    ELSE 'Другой'
                END as city
            FROM stores
        ),
        order_details AS (
            SELECT 
                DATE(o.created_at) as report_date,
                EXTRACT(YEAR FROM o.created_at) as report_year,
                EXTRACT(MONTH FROM o.created_at) as report_month,
                EXTRACT(DAY FROM o.created_at) as report_day,
                ce.city,
                o.store_id,
                o.order_id,
                o.user_id,
                -- Рассчитываем стоимость заказа с учетом скидок
                SUM(oi.item_price * oi.item_quantity * 
                    (1 - COALESCE(oi.item_discount, 0)/100.0) *
                    (1 - COALESCE(o.order_discount, 0)/100.0)) as order_value,
                -- Флаги статусов
                CASE WHEN o.delivered_at IS NOT NULL THEN 1 ELSE 0 END as is_delivered,
                CASE WHEN o.canceled_at IS NOT NULL THEN 1 ELSE 0 END as is_canceled,
                -- Выручка: только доставленные заказы
                CASE 
                    WHEN o.delivered_at IS NOT NULL 
                    THEN SUM(oi.item_price * oi.item_quantity * 
                           (1 - COALESCE(oi.item_discount, 0)/100.0) *
                           (1 - COALESCE(o.order_discount, 0)/100.0))
                    ELSE 0 
                END as revenue
            FROM orders o
            JOIN city_extracted ce ON o.store_id = ce.store_id
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.created_at >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY 1,2,3,4,5,6,7,8
        )
        SELECT 
            report_date,
            report_year,
            report_month,
            report_day,
            city,
            store_id,
            COUNT(DISTINCT user_id) as unique_customers,
            COUNT(DISTINCT order_id) as total_orders,
            SUM(is_delivered) as total_delivered_orders,
            SUM(is_canceled) as total_canceled_orders,
            SUM(order_value) as total_turnover,
            SUM(revenue) as total_revenue,
            AVG(order_value) as avg_order_value
        FROM order_details
        GROUP BY 1,2,3,4,5,6
        ON CONFLICT (report_date, city, store_id) 
        DO UPDATE SET
            unique_customers = EXCLUDED.unique_customers,
            total_orders = EXCLUDED.total_orders,
            total_delivered_orders = EXCLUDED.total_delivered_orders,
            total_canceled_orders = EXCLUDED.total_canceled_orders,
            total_turnover = EXCLUDED.total_turnover,
            total_revenue = EXCLUDED.total_revenue,
            avg_order_value = EXCLUDED.avg_order_value,
            calculated_at = CURRENT_TIMESTAMP;
        '''
    )
    
    # 3. Построение витрины товаров (dm_items)
    build_items_datamart = PostgresOperator(
        task_id='build_items_datamart',
        postgres_conn_id='main_postgres',
        sql='''
        -- ВИТРИНА ТОВАРОВ
        INSERT INTO dm_items (
            report_date, report_year, report_month, report_day,
            city, store_id, item_category, item_id,
            item_turnover, ordered_quantity, canceled_quantity,
            orders_with_item
        )
        WITH city_extracted AS (
            SELECT 
                store_id,
                CASE 
                    WHEN store_address LIKE '%Москва%' THEN 'Москва'
                    WHEN store_address LIKE '%Санкт-Петербург%' THEN 'Санкт-Петербург'
                    WHEN store_address LIKE '%Новосибирск%' THEN 'Новосибирск'
                    WHEN store_address LIKE '%Екатеринбург%' THEN 'Екатеринбург'
                    WHEN store_address LIKE '%Казань%' THEN 'Казань'
                    ELSE 'Другой'
                END as city
            FROM stores
        ),
        item_details AS (
            SELECT 
                DATE(o.created_at) as report_date,
                EXTRACT(YEAR FROM o.created_at) as report_year,
                EXTRACT(MONTH FROM o.created_at) as report_month,
                EXTRACT(DAY FROM o.created_at) as report_day,
                ce.city,
                o.store_id,
                i.item_category,
                oi.item_id,
                -- Метрики товара
                SUM(oi.item_price * oi.item_quantity * 
                    (1 - COALESCE(oi.item_discount, 0)/100.0)) as item_turnover,
                SUM(oi.item_quantity) as ordered_quantity,
                SUM(COALESCE(oi.item_canceled_quantity, 0)) as canceled_quantity,
                COUNT(DISTINCT o.order_id) as orders_count
            FROM orders o
            JOIN city_extracted ce ON o.store_id = ce.store_id
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN items i ON oi.item_id = i.item_id
            WHERE o.created_at >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY 1,2,3,4,5,6,7,8
        )
        SELECT 
            report_date,
            report_year,
            report_month,
            report_day,
            city,
            store_id,
            item_category,
            item_id,
            SUM(item_turnover) as item_turnover,
            SUM(ordered_quantity) as ordered_quantity,
            SUM(canceled_quantity) as canceled_quantity,
            SUM(orders_count) as orders_with_item
        FROM item_details
        GROUP BY 1,2,3,4,5,6,7,8
        ON CONFLICT (report_date, item_id, store_id, city) 
        DO UPDATE SET
            item_turnover = EXCLUDED.item_turnover,
            ordered_quantity = EXCLUDED.ordered_quantity,
            canceled_quantity = EXCLUDED.canceled_quantity,
            orders_with_item = EXCLUDED.orders_with_item,
            calculated_at = CURRENT_TIMESTAMP;
        '''
    )
    
    # Порядок выполнения задач
    truncate_datamarts >> build_orders_datamart >> build_items_datamart

-- ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ДОСТАВОК

DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS items CASCADE;
DROP TABLE IF EXISTS drivers CASCADE;
DROP TABLE IF EXISTS stores CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- 1. ТАБЛИЦА ПОЛЬЗОВАТЕЛЕЙ
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    user_phone VARCHAR(50) NOT NULL
);

-- 2. ТАБЛИЦА МАГАЗИНОВ
CREATE TABLE stores (
    store_id INT PRIMARY KEY,
    store_address TEXT NOT NULL
);

-- 3. ТАБЛИЦА КУРЬЕРОВ
CREATE TABLE drivers (
    driver_id INT PRIMARY KEY,
    driver_phone VARCHAR(50) NOT NULL
);

-- 4. ТАБЛИЦА ТОВАРОВ
CREATE TABLE items (
    item_id INT PRIMARY KEY,
    item_title VARCHAR(500) NOT NULL,
    item_category VARCHAR(200) NOT NULL
);

-- 5. ТАБЛИЦА ЗАКАЗОВ
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    store_id INT NOT NULL REFERENCES stores(store_id),
    driver_id INT REFERENCES drivers(driver_id),
    address_text TEXT,
    created_at TIMESTAMP NOT NULL,
    paid_at TIMESTAMP,
    delivery_started_at TIMESTAMP,
    delivered_at TIMESTAMP,
    canceled_at TIMESTAMP,
    payment_type VARCHAR(50),
    order_discount DECIMAL(5,2) DEFAULT 0.00,
    delivery_cost DECIMAL(10,2) DEFAULT 0.00,
    order_cancellation_reason TEXT
);

-- 6. ТАБЛИЦА ТОВАРОВ В ЗАКАЗАХ
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(order_id),
    item_id INT NOT NULL REFERENCES items(item_id),
    item_quantity INT NOT NULL,
    item_canceled_quantity INT DEFAULT 0,
    item_price DECIMAL(10,2) NOT NULL,
    item_discount DECIMAL(5,2) DEFAULT 0.00,
    item_replaced_id INT REFERENCES items(item_id)
);

-- 7. ВИТРИНА ЗАКАЗОВ
CREATE TABLE dm_orders (
    report_date DATE NOT NULL,
    report_year INT,
    report_month INT,
    report_day INT,
    city VARCHAR(100),
    store_id INT,
    unique_customers INT DEFAULT 0,
    total_orders INT DEFAULT 0,
    total_delivered_orders INT DEFAULT 0,
    total_canceled_orders INT DEFAULT 0,
    total_turnover DECIMAL(15,2) DEFAULT 0.00,
    total_revenue DECIMAL(15,2) DEFAULT 0.00,
    avg_order_value DECIMAL(10,2) DEFAULT 0.00,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(report_date, city, store_id)
);

-- 8. ВИТРИНА ТОВАРОВ
CREATE TABLE dm_items (
    report_date DATE NOT NULL,
    report_year INT,
    report_month INT,
    report_day INT,
    city VARCHAR(100),
    store_id INT,
    item_category VARCHAR(200),
    item_id INT NOT NULL,
    item_turnover DECIMAL(15,2) DEFAULT 0.00,
    ordered_quantity INT DEFAULT 0,
    canceled_quantity INT DEFAULT 0,
    orders_with_item INT DEFAULT 0,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(report_date, item_id, store_id, city)
);

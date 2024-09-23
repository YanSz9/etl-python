CREATE TABLE d_product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    product_category VARCHAR(255),
    product_description VARCHAR(255),
    list_price DECIMAL(18, 2),
    standard_cost DECIMAL(18, 2),
    color VARCHAR(50),
    size VARCHAR(50),
    weight DECIMAL(18, 2)
);

CREATE TABLE f_sales (
    sales_order_id INT PRIMARY KEY,
    revision_number INT,
    order_date DATE,
    due_date DATE,
    ship_date DATE,
    status VARCHAR(50),
    is_ordered_online BOOLEAN,
    sales_order_number VARCHAR(50),
    purchase_order_number VARCHAR(50),
    account_number VARCHAR(50),
    customer_id INT,
    sales_person_id INT,
    territory VARCHAR(50),
    bill_to_address TEXT,
    ship_to_address TEXT,
    ship_method VARCHAR(50),
    credit_card_id VARCHAR(50),
    credit_card_approval_code VARCHAR(50),
    currency_rate_id VARCHAR(50),
    sub_total DECIMAL(18, 2),
    tax_amount DECIMAL(18, 2),
    freight DECIMAL(18, 2),
    total_due DECIMAL(18, 2),
    comment TEXT,
    modified_date TIMESTAMP
);

CREATE TABLE d_sales_order_details (
    sales_order_id INT,
    sales_order_detail_id INT PRIMARY KEY,
    carrier_tracking_number VARCHAR(50),
    order_quantity INT,
    product_id INT,
    product_name VARCHAR(255),
    special_offer_id INT,
    unit_price DECIMAL(18, 2),
    unit_price_discount DECIMAL(18, 2),
    line_total DECIMAL(18, 2),
    modified_date TIMESTAMP
);

CREATE TABLE d_calendario (
    data DATE PRIMARY KEY,
    ano INT,
    mes INT,
    dia INT,
    trimestre INT,
    nome_mes VARCHAR(20),
    nome_dia_semana VARCHAR(20),
    dia_semana INT,
    semana_ano INT,
    fim_semana BOOLEAN
);
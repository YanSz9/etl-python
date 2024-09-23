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
    order_date DATE, 
    due_date DATE, 
    ship_date DATE, 
    customer_id INT,
    sales_person_id INT,
    product_id INT, 
    sub_total DECIMAL(18, 2),
    tax_amount DECIMAL(18, 2),
    freight DECIMAL(18, 2),
    total_due DECIMAL(18, 2),
    modified_date TIMESTAMP,
    FOREIGN KEY (order_date) REFERENCES d_calendario(data),
    FOREIGN KEY (due_date) REFERENCES d_calendario(data),
    FOREIGN KEY (ship_date) REFERENCES d_calendario(data),
    FOREIGN KEY (product_id) REFERENCES d_product(product_id)
);

CREATE TABLE d_sales_order_details (
    sales_order_detail_id INT PRIMARY KEY,
    sales_order_id INT, 
    product_id INT,
    carrier_tracking_number VARCHAR(50),
    order_quantity INT,
    unit_price DECIMAL(18, 2),
    unit_price_discount DECIMAL(18, 2),
    line_total DECIMAL(18, 2),
    modified_date TIMESTAMP,
    FOREIGN KEY (sales_order_id) REFERENCES f_sales(sales_order_id),
    FOREIGN KEY (product_id) REFERENCES d_product(product_id)
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
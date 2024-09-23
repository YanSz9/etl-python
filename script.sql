-- SCRIPT DE CRIAÇÃO DAS TABELAS 

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

--SELECTS DAS TABELAS

-- Vendas por produto
SELECT
    p.product_name AS "Product",
    COUNT(sod.sales_order_id) AS "Sales Quantity",
    SUM(sod.line_total) AS "Revenue Generate"
FROM
    d_product p
        JOIN
    d_sales_order_details sod ON p.product_id = sod.product_id
        JOIN
    f_sales s ON sod.sales_order_id = s.sales_order_id
GROUP BY
    p.product_name
ORDER BY
    "Revenue Generate" DESC;


-- Vendas por data
SELECT
    s.order_date AS "Date",
    COUNT(s.sales_order_id) AS "Sales Number",
    SUM(s.sub_total) AS "Total Sold"
FROM
    f_sales s
GROUP BY
    s.order_date
ORDER BY
    s.order_date DESC;


-- Vendas por cliente
SELECT
    s.customer_id AS "Client ID",
    COUNT(s.sales_order_id) AS "Sales Number",
    SUM(s.total_due) AS "Total Spent"
FROM
    f_sales s
GROUP BY
    s.customer_id
ORDER BY
    "Total Spent" DESC;

-- Detalhes do Pedido 
SELECT
    s.sales_order_number AS "Order Number",
    p.product_name AS "Product",
    sod.order_quantity AS "Quantity",
    sod.unit_price AS "Unit price",
    sod.unit_price_discount AS "Discount",
    sod.line_total AS "Total Line"
FROM
    f_sales s
        JOIN
    d_sales_order_details sod ON s.sales_order_id = sod.sales_order_id
        JOIN
    d_product p ON sod.product_id = p.product_id
WHERE
    s.sales_order_number = 'SO43659';


-- Vendas por categoria de produto 
SELECT
    p.product_category AS "Category",
    COUNT(sod.sales_order_id) AS "Sales Number",
    SUM(sod.line_total) AS "Revenue Generated"
FROM
    d_product p
        JOIN
    d_sales_order_details sod ON p.product_id = sod.product_id
        JOIN
    f_sales s ON sod.sales_order_id = s.sales_order_id
GROUP BY
    p.product_category
ORDER BY
    "Revenue Generated" DESC;

-- Produtos mais vendidos
SELECT
    p.product_name AS "Product",
    SUM(sod.order_quantity) AS "Quantity Sold"
FROM
    d_product p
        JOIN
    d_sales_order_details sod ON p.product_id = sod.product_id
GROUP BY
    p.product_name
ORDER BY
    "Quantity Sold" DESC;


-- Vendas Online x Offline
SELECT
    CASE
        WHEN s.is_ordered_online THEN 'Online'
        ELSE 'Offline'
        END AS "Sales Type",
    COUNT(s.sales_order_id) AS "Sales Order",
    SUM(s.total_due) AS "Total Sales"
FROM
    f_sales s
GROUP BY
    "Sales Type";


-- Frete médio por entrega
SELECT
    s.ship_method AS "Delivery Method",
    AVG(s.freight) AS "Medium Freight",
    SUM(s.freight) AS "Total Freight"
FROM
    f_sales s
GROUP BY
    s.ship_method
ORDER BY
    "Medium Freight" DESC;


-- Vendas por vendedor 
SELECT
    s.sales_person_id AS "Seller ID",
    COUNT(s.sales_order_id) AS "Sales Number",
    SUM(s.total_due) AS "Total Sales"
FROM
    f_sales s
GROUP BY
    s.sales_person_id
ORDER BY
    "Total Sales" DESC;


-- Vendas por território
SELECT
    s.territory AS "Territory",
    COUNT(s.sales_order_id) AS "Sales Number",
    SUM(s.total_due) AS "Total Sales"
FROM
    f_sales s
GROUP BY
    s.territory
ORDER BY
    "Total Sales" DESC;

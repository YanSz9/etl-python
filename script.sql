-- SCRIPT DE CRIAÇÃO DAS TABELAS 

CREATE TABLE "d_product" (
"product_id" INT PRIMARY KEY,
"product_name" VARCHAR(255),
"product_category" VARCHAR(255),
"product_description" VARCHAR(255),
"list_price" DECIMAL(18,2),
"standard_cost" DECIMAL(18,2),
"color" VARCHAR(50),
"size" VARCHAR(50),
"weight" DECIMAL(18,2)
);

CREATE TABLE "d_persons" (
"person_id" SERIAL PRIMARY KEY,
"person_type" VARCHAR(255),
"name_style" VARCHAR(255),
"title" VARCHAR(100),
"first_name" VARCHAR(100),
"middle_name" VARCHAR(100),
"last_name" VARCHAR(100),
"suffix" VARCHAR(50),
"email_promotion" VARCHAR(255),
"additional_contact_info" TEXT,
"modified_date" DATE
);

CREATE TABLE "d_customers" (
"customer_id" SERIAL PRIMARY KEY,
"store_id" INTEGER,
"territory" VARCHAR(255),
"account_number" VARCHAR(50),
"modified_date" TIMESTAMP
);

CREATE TABLE "f_sales" (
"sales_order_id" INT PRIMARY KEY,
"customer_id" INT,
"person_id" int,
"product_id" int,
"sales_order_details_id" int,
"revision_number" INT,
"order_date" DATE,
"due_date" DATE,
"ship_date" DATE,
"status" VARCHAR(50),
"is_ordered_online" BOOLEAN,
"sales_order_number" VARCHAR(50),
"purchase_order_number" VARCHAR(50),
"account_number" VARCHAR(50),
"sales_person_id" INT,
"territory" VARCHAR(50),
"bill_to_address" TEXT,
"ship_to_address" TEXT,
"ship_method" VARCHAR(50),
"credit_card_id" VARCHAR(50),
"credit_card_approval_code" VARCHAR(50),
"currency_rate_id" VARCHAR(50),
"sub_total" DECIMAL(18,2),
"tax_amount" DECIMAL(18,2),
"freight" DECIMAL(18,2),
"total_due" DECIMAL(18,2),
"comment" TEXT,
"modified_date" TIMESTAMP
);

CREATE TABLE "d_sales_order_details" (
"sales_order_detail_id" INT PRIMARY KEY,
"carrier_tracking_number" VARCHAR(50),
"order_quantity" INT,
"product_name" VARCHAR(255),
"special_offer_id" INT,
"unit_price" DECIMAL(18,2),
"unit_price_discount" DECIMAL(18,2),
"line_total" DECIMAL(18,2),
"modified_date" TIMESTAMP
);

ALTER TABLE "f_sales" ADD FOREIGN KEY ("customer_id") REFERENCES "d_customers" ("customer_id");

ALTER TABLE "f_sales" ADD FOREIGN KEY ("person_id") REFERENCES "d_persons" ("person_id");

ALTER TABLE "f_sales" ADD FOREIGN KEY ("product_id") REFERENCES "d_product" ("product_id");

ALTER TABLE "f_sales" ADD FOREIGN KEY ("sales_order_details_id") REFERENCES "d_sales_order_details" ("sales_order_detail_id");

--SELECTS DAS TABELAS

SELECT 
    p.product_name,
    SUM(f.total_due) AS total_sales
FROM 
    f_sales f
JOIN 
    d_product p ON f.product_id = p.product_id
GROUP BY 
    p.product_name
ORDER BY 
    total_sales DESC;


SELECT 
    c.account_number,
    c.territory,
    SUM(f.total_due) AS total_sales
FROM 
    f_sales f
JOIN 
    d_customers c ON f.customer_id = c.customer_id
GROUP BY 
    c.account_number, c.territory
ORDER BY 
    total_sales DESC;


SELECT 
    DATE_TRUNC('month', f.order_date) AS sales_month,
    SUM(f.total_due) AS total_sales
FROM 
    f_sales f
GROUP BY 
    sales_month
ORDER BY 
    sales_month;


SELECT 
    f.sales_order_id,
    p.product_name,
    c.account_number,
    f.order_date,
    f.total_due
FROM 
    f_sales f
JOIN 
    d_product p ON f.product_id = p.product_id
JOIN 
    d_customers c ON f.customer_id = c.customer_id
ORDER BY 
    f.order_date DESC;


SELECT 
    pe.person_type,
    SUM(f.total_due) AS total_sales
FROM 
    f_sales f
JOIN 
    d_persons pe ON f.person_id = pe.person_id
JOIN 
    d_customers c ON f.customer_id = c.customer_id
GROUP BY 
    pe.person_type
ORDER BY 
    total_sales DESC;


SELECT 
    f.is_ordered_online,
    SUM(f.total_due) AS total_sales
FROM 
    f_sales f
GROUP BY 
    f.is_ordered_online
ORDER BY 
    f.is_ordered_online;



SELECT 
    d.product_name,
    d.product_category,
    AVG(dod.unit_price_discount) AS average_discount
FROM 
    d_sales_order_details dod
JOIN 
    d_product d ON dod.product_id = d.product_id
GROUP BY 
    d.product_name, d.product_category
HAVING 
    AVG(dod.unit_price_discount) > 0
ORDER BY 
    average_discount DESC;
g
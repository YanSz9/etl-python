import requests
import psycopg2 as db
from psycopg2 import sql
from dotenv import load_dotenv
import os

load_dotenv()

def get_database_connection(conn_params):
    return db.connect(**conn_params)

def close_database_connection(conn):
    if conn is not None:
        conn.close()

def execute_query(cursor, query, data):
    try:
        cursor.execute(query, data)
    except Exception as e:
        print(f"Error executing query: {e}")
        raise

def extract_data(url, page_size):
    page_number = 1
    while True:
        full_url = f"{url}?PageNumber={page_number}&PageSize={page_size}"
        print(f"Fetching data from URL: {full_url}")
        response = requests.get(full_url)
        if response.status_code == 200:
            data = response.json()
            print(f"Data from page {page_number}: {data}")
            if isinstance(data, list) and not data:
                print("No more data to fetch.")
                break
            yield data
            page_number += 1
        else:
            print(f"Error fetching data: {response.status_code}")
            print(f"Response text: {response.text}")
            raise Exception(f"Error fetching data: {response.status_code}")

def transform_data(raw_data, mapping):
    transformed_data = []
    for item in raw_data:
        try:
            transformed_item = {key: item.get(value) for key, value in mapping.items()}
            print(f"Transformed item: {transformed_item}")
            transformed_data.append(transformed_item)
        except KeyError as e:
            print(f"Missing key: {e} in item: {item}")
    return transformed_data

def load_data(transformed_data, conn_params, table_name, columns, conflict_column):
    conn = get_database_connection(conn_params)
    cursor = conn.cursor()
    
    columns_str = ', '.join(columns)
    values_str = ', '.join([f"%s" for _ in columns])
    update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_column])
    
    query = sql.SQL(f'''
        INSERT INTO {table_name} ({columns_str})
        VALUES ({values_str})
        ON CONFLICT ({conflict_column}) DO UPDATE 
        SET {update_str};
    ''')
    
    try:
        for item in transformed_data:
            print(f"Inserting data into database: {item}")
            execute_query(cursor, query, tuple(item.values()))
        conn.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        cursor.close()
        close_database_connection(conn)

conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

product_url = "https://demodata.grapecity.com/adventureworks/api/v1/products"
sales_url = "https://demodata.grapecity.com/adventureworks/api/v1/salesOrders"
sales_order_details_url = "https://demodata.grapecity.com/adventureworks/api/v1/salesOrderDetails"
customers_url = "https://demodata.grapecity.com/adventureworks/api/v1/customers"
persons_url = 'https://demodata.grapecity.com/adventureworks/api/v1/persons'
page_size = 500

product_mapping = {
    'ProductID': 'productId',
    'ProductName': 'name',
    'ProductCategory': 'category',
    'ProductDescription': 'model',
    'ListPrice': 'listPrice',
    'StandardCost': 'standardCost',
    'Color': 'color',
    'Size': 'size',
    'Weight': 'weight'
}

sales_mapping = {
    'SalesOrderID': 'salesOrderId',
    'RevisionNumber': 'revisionNumber',
    'OrderDate': 'orderDate',
    'DueDate': 'dueDate',
    'ShipDate': 'shipDate',
    'Status': 'status',
    'IsOrderedOnline': 'isOrderedOnline',
    'SalesOrderNumber': 'salesOrderNumber',
    'PurchaseOrderNumber': 'purchaseOrderNumber',
    'AccountNumber': 'accountNumber',
    'CustomerID': 'customerId',
    'SalesPersonID': 'salesPersonId',
    'Territory': 'territory',
    'BillToAddress': 'billToAddress',
    'ShipToAddress': 'shipToAddress',
    'ShipMethod': 'shipMethod',
    'CreditCardID': 'creditCardId',
    'CreditCardApprovalCode': 'creditCardApprovalCode',
    'CurrencyRateID': 'currencyRateId',
    'SubTotal': 'subTotal',
    'TaxAmount': 'taxAmount',
    'Freight': 'freight',
    'TotalDue': 'totalDue',
    'Comment': 'comment',
    'ModifiedDate': 'modifiedDate'
}

sales_order_details_mapping = {
    'SalesOrderID': 'salesOrderId',
    'SalesOrderDetailID': 'salesOrderDetailId',
    'CarrierTrackingNumber': 'carrierTrackingNumber',
    'OrderQuantity': 'orderQuantity',
    'ProductID': 'productId',
    'ProductName': 'productName',
    'SpecialOfferID': 'specialOfferId',
    'UnitPrice': 'unitPrice',
    'UnitPriceDiscount': 'unitPriceDiscount',
    'LineTotal': 'lineTotal',
    'ModifiedDate': 'modifiedDate'
}

customers_mapping = {
    'CustomerID': 'customerId',
    'PersonID': 'personId',
    'StoreID': 'storeId',
    'Territory': 'territory',
    'AccountNumber': 'accountNumber',
    'ModifiedDate': 'modifiedDate'
}

persons_mapping = {
    'PersonID': 'personId',
    'PersonType': 'personType',
    'NameStyle': 'nameStyle',
    'Title': 'title',
    'FirstName': 'firstName',
    'MiddleName': 'middleName',
    'LastName': 'lastName',
    'Suffix': 'suffix',
    'EmailPromotion': 'emailPromotion',
    'AdditionalContactInfo': 'additionalContactInfo',
    'ModifiedDate': 'modifiedDate'
}

product_columns = [
    'product_id', 'product_name', 'product_category', 'product_description', 'list_price',
    'standard_cost', 'color', 'size', 'weight'
]

sales_columns = [
    'sales_order_id', 'revision_number', 'order_date', 'due_date', 'ship_date', 'status',
    'is_ordered_online', 'sales_order_number', 'purchase_order_number', 'account_number',
    'customer_id', 'sales_person_id', 'territory', 'bill_to_address', 'ship_to_address',
    'ship_method', 'credit_card_id', 'credit_card_approval_code', 'currency_rate_id',
    'sub_total', 'tax_amount', 'freight', 'total_due', 'comment', 'modified_date'
]

sales_order_details_columns = [
    'sales_order_id', 'sales_order_detail_id', 'carrier_tracking_number', 'order_quantity',
    'product_id', 'product_name', 'special_offer_id', 'unit_price', 'unit_price_discount',
    'line_total', 'modified_date'
]

persons_columns = [
    'person_id', 'person_type', 'name_style', 'title', 'first_name',
    'middle_name', 'last_name', 'suffix', 'email_promotion', 
    'additional_contact_info', 'modified_date'
]

customers_columns = [
    'customer_id', 'person_id', 'store_id', 'territory', 'account_number', 'modified_date'
]

try:
    for products in extract_data(product_url, page_size):
        transformed_products = transform_data(products, product_mapping)
        if transformed_products:
            load_data(transformed_products, conn_params, 'd_product', product_columns, 'product_id')
    
    for sales in extract_data(sales_url, page_size):
        transformed_sales = transform_data(sales, sales_mapping)
        if transformed_sales:
            load_data(transformed_sales, conn_params, 'f_sales', sales_columns, 'sales_order_id')
    
    for customers in extract_data(customers_url, page_size):  
        transformed_customers = transform_data(customers, customers_mapping) 
        if transformed_customers:
            load_data(transformed_customers, conn_params, 'd_customers', customers_columns, 'customer_id')
    
    for persons in extract_data(persons_url, page_size):  
        transformed_persons = transform_data(persons, persons_mapping) 
        if transformed_persons:
            load_data(transformed_persons, conn_params, 'd_persons', persons_columns, 'person_id')

    for sales_order_details in extract_data(sales_order_details_url, page_size):
        transformed_sales_order_details = transform_data(sales_order_details, sales_order_details_mapping)
        if transformed_sales_order_details:
            load_data(transformed_sales_order_details, conn_params, 'd_sales_order_details', sales_order_details_columns, 'sales_order_detail_id')

except Exception as e:
    print(f"An error occurred: {e}")
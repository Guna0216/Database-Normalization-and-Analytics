### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql = '''CREATE TABLE Region (
                RegionID INTEGER NOT NULL PRIMARY KEY, 
                Region TEXT NOT NULL);'''
    create_table(conn, sql)
    def insert_values(conn, values):
        sql = ''' INSERT INTO Region (Region) VALUES(?) '''
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid
    with open(data_filename, 'r') as file:
        data = []
        for line in file:
            data.append(line.strip().split('\t')[4])
        data = list(set(data[1:]))
        data.sort()
        data = [(ele,) for ele in data]
    with conn:
        insert_values(conn, data)
    
    ### END SOLUTION

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql = 'SELECT DISTINCT(Region), RegionID FROM Region'
    regions = execute_sql_statement(sql, conn)
    return dict(regions)

    ### END SOLUTION


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql = '''CREATE TABLE Country (
                CountryID INTEGER NOT NULL PRIMARY KEY, 
                Country TEXT NOT NULL,
                RegionID INTEGER NOT NULL, 
                FOREIGN KEY (RegionID) REFERENCES Region(RegionID));'''
    create_table(conn, sql)
    def insert_values(conn, values):
        sql = ''' INSERT INTO Country (Country, RegionID) VALUES(?, ?) '''
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid
    with open(data_filename, 'r') as file:
        data = []
        for line in file:
            data.append(line.strip().split('\t')[3:5])
        data = data[1:]
        data.sort()
        region_dict = step2_create_region_to_regionid_dictionary(normalized_database_filename)
        country_region = {ele[0]: region_dict[ele[1]] for ele in data}
        country_region = list(country_region.items())
    with conn:
        insert_values(conn, country_region)        
    ### END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql = 'SELECT DISTINCT(Country), CountryID FROM Country'
    countries = execute_sql_statement(sql, conn)
    return dict(countries)
    ### END SOLUTION
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):

    ### BEGIN SOLUTION  
    conn = create_connection(normalized_database_filename)
    sql = '''CREATE TABLE Customer (
                    CustomerID INTEGER NOT NULL PRIMARY KEY, 
                    FirstName TEXT NOT NULL,
                    LastName TEXT NOT NULL,
                    Address TEXT NOT NULL,
                    City TEXT NOT NULL,
                    CountryID INTEGER NOT NULL, 
                    FOREIGN KEY (CountryID) REFERENCES Country (CountryID));'''
    create_table(conn, sql)
    def insert_values(conn, values):
        sql = ''' INSERT INTO Customer (FirstName, LastName, Address, City, CountryID) VALUES(?, ?, ?, ?, ?);'''
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid
    with open(data_filename, 'r') as file:
        data = []
        for line in file:
            data.append(line.strip().split('\t')[:4])
        data = data[1:]
        data.sort()
        country_ids = step4_create_country_to_countryid_dictionary(normalized_database_filename)
        data_pp = [ele[:3]+[country_ids[ele[3]]] for ele in data]
        data_pp = [tuple(ele[0].split(' ',1)) + tuple(ele[1:]) for ele in data_pp]
    with conn:
        insert_values(conn, data_pp)
    ### END SOLUTION


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql = 'SELECT FirstName || " " || LastName, CustomerID FROM Customer;'
    customers = execute_sql_statement(sql, conn)
    return dict(customers)
    ### END SOLUTION
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql = '''CREATE TABLE ProductCategory (
                    ProductCategoryID integer not null Primary Key,
                    ProductCategory Text not null,
                    ProductCategoryDescription Text not null);'''
    create_table(conn, sql)
    def insert_values(conn, values):
        sql = ''' INSERT INTO ProductCategory (ProductCategory, ProductCategoryDescription) VALUES(?, ?);'''
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid
    with open(data_filename, 'r') as file:
        data = []
        for line in file:
            data.append(line.strip().split('\t')[6:8])
        data = data[1:]
        data.sort()
        data = [[ele[0].split(';'),ele[1].split(';')] for ele in data]
        data = [dict(zip(ele[0],ele[1])) for ele in data]
        product_dict = {}
        for ele in data:
            product_dict.update(ele)
        product_values = list(product_dict.items())
        product_values.sort()
    with conn:
        insert_values(conn, product_values)
   
    ### END SOLUTION

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql = 'SELECT ProductCategory, ProductCategoryID FROM ProductCategory;'
    product_categories = execute_sql_statement(sql, conn)
    return dict(product_categories)

    ### END SOLUTION
        

def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql = '''CREATE TABLE Product (
                    ProductID integer not null Primary key,
                    ProductName Text not null,
                    ProductUnitPrice Real not null,
                    ProductCategoryID integer not null,
                    foreign key (ProductCategoryID) REFERENCES ProductCategory (ProductCategoryID));'''
    create_table(conn, sql)
    def insert_values(conn, values):
        sql = ''' INSERT INTO Product (ProductName, ProductUnitPrice, ProductCategoryID) VALUES(?, ?, ?);'''
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid
    categories = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
    with open(data_filename, 'r') as file:
        data = []
        for line in file:
            data.append(line.strip().split('\t')[5:9])
        data = data[1:]
        data = [[ele[0].split(';'),ele[1].split(';'),ele[3].split(';')] for ele in data]
        data_cat = [dict(zip(ele[0],ele[1])) for ele in data]
        data_price = [dict(zip(ele[0],ele[2])) for ele in data]
        my_dict = {}
        prices = {}
        for ele in data_cat:
            my_dict.update(ele)
        my_dict_cat = {key:categories[value] for key,value in my_dict.items()}
        for ele in data_price:
            prices.update(ele)
        my_dict_price = {key:value for key,value in prices.items()}
        product_cat_price = [(ele, my_dict_price[ele],my_dict_cat[ele]) for ele in my_dict_cat.keys()]
        product_cat_price.sort()
        with conn:
            insert_values(conn, product_cat_price)
    ### END SOLUTION


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql = 'SELECT ProductName, ProductID FROM Product;'
    products = execute_sql_statement(sql, conn)
    return dict(products)

    ### END SOLUTION
        
import datetime
def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn_norm = create_connection(normalized_database_filename)   
    data = []

    with open(data_filename) as f:
        next(f)
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            line = line.split('\t')
            
            quantities_ordered = line[9].split(';')
            order_date = line[10].split(';')
            formatted_date = []
            for i in order_date:
                i = datetime.datetime.strptime(i, '%Y%m%d').strftime('%Y-%m-%d')
                formatted_date.append(i)
            product = line[5].split(';')
            customer_name = [line[0]]*len(formatted_date)

            orddet_data = list(zip(customer_name, product, formatted_date, quantities_ordered))
            for i in orddet_data:
                data.append(i)
        orddet_data = data
        prod_data = step10_create_product_to_productid_dictionary(normalized_database_filename)
        cust_data = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
        
        orddet_table_output = []
        for i in orddet_data:
            cust_id = cust_data[i[0]]
            prod_id = prod_data[i[1]]
            orddet_table_output.append((cust_id, prod_id, i[2], int(i[3])))
        
        create_orddet_query = '''create table if not exists OrderDetail (
            OrderID integer primary key not null, 
            CustomerID inetger not null, 
            ProductID integer not null, 
            OrderDate integer not null, 
            QuantityOrdered integer not null, 
            foreign key(CustomerID) references Customer(CustomerID), 
            foreign key(ProductID) references Product(ProductID));'''
        create_table(conn_norm, create_orddet_query)

        def insert_product(conn_norm, values):
            sql_statement = "insert into OrderDetail(CustomerID, ProductID, OrderDate, QuantityOrdered) values(?, ?, ?, ?);"
            cur = conn_norm.cursor()
            cur.executemany(sql_statement, values)
            return cur.lastrowid
        
        with conn_norm:
            insert_product(conn_norm, orddet_table_output)
    ### END SOLUTION


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    customers = step6_create_customer_to_customerid_dictionary('normalized.db')
    cust_id = customers[CustomerName]
    sql_statement = """
    SELECT
    c.FirstName || ' ' || c.LastName AS Name, 
    p.ProductName,
    o.OrderDate,
    p.ProductUnitPrice,
    o.QuantityOrdered,
    ROUND(p.ProductUnitPrice * o.QuantityOrdered, 2) AS Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p on o.ProductID = p.ProductID
    where c.CustomerID = {}
    """.format(cust_id)
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    customers = step6_create_customer_to_customerid_dictionary('normalized.db')
    cust_id = customers[CustomerName]
    sql_statement = """
    SELECT
    c.FirstName || ' ' || c.LastName AS Name, 
    ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered),2) AS Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p on o.ProductID = p.ProductID
    where c.CustomerID = {}
    GROUP BY 1
    """.format(cust_id)
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION
    sql_statement = """
    SELECT
    c.FirstName || ' ' || c.LastName AS Name,
    ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered),2) AS Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p on o.ProductID = p.ProductID
    GROUP BY 1
    ORDER BY 2 DESC
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """
    SELECT
    r.Region,
    ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered),2) AS Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p on o.ProductID = p.ProductID
    JOIN Country ct on c.CountryID = ct.CountryID
    JOIN Region r on ct.RegionID = r.RegionID
    GROUP BY 1
    ORDER BY 2 DESC
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex5(conn):
    
     # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """
    SELECT
    ct.Country,
    ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) AS CountryTotal
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p on o.ProductID = p.ProductID
    JOIN Country ct on c.CountryID = ct.CountryID
    GROUP BY 1
    ORDER BY 2 DESC
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = """
    SELECT
    r.Region,
    ct.Country,
    ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) AS CountryTotal,
    rank() OVER (PARTITION BY r.Region ORDER BY ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) DESC) CountryRegionalRank
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p on o.ProductID = p.ProductID
    JOIN Country ct on c.CountryID = ct.CountryID
    JOIN Region r on ct.RegionID = r.RegionID
    GROUP BY 1,2
    ORDER BY 1 ASC
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = """
    WITH Country_ranks AS (
    SELECT
    r.Region,
    ct.Country,
    ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) AS CountryTotal,
    rank() OVER (PARTITION BY r.Region ORDER BY ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) DESC) CountryRegionalRank
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p on o.ProductID = p.ProductID
    JOIN Country ct on c.CountryID = ct.CountryID
    JOIN Region r on ct.RegionID = r.RegionID
    GROUP BY 1,2
    ORDER BY 1 ASC
    )
    SELECT * FROM Country_ranks WHERE CountryRegionalRank = 1
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """
    WITH Customer_sales AS (
    SELECT 
    CASE
        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 1
        AND 3 THEN 'Q1'
        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 4
        AND 6 THEN 'Q2'
        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 7
        AND 9 THEN 'Q3'
        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 10
        AND 12 THEN 'Q4'
    END Quarter,
    cast(strftime('%Y', o.OrderDate) as INT) Year,
    c.CustomerID,
    ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) as Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p on o.ProductID = p.ProductID
    GROUP BY 1,2,3
    ORDER BY 2
    )  
    SELECT * FROM Customer_sales
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = """
    WITH Customer_sales AS (
    SELECT 
    CASE
        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 1
        AND 3 THEN 'Q1'
        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 4
        AND 6 THEN 'Q2'
        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 7
        AND 9 THEN 'Q3'
        WHEN 0 + strftime('%m', o.OrderDate) BETWEEN 10
        AND 12 THEN 'Q4'
    END Quarter,
    cast(strftime('%Y', o.OrderDate) as INT) Year,
    c.CustomerID,
    ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) as Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p on o.ProductID = p.ProductID
    GROUP BY 1,2,3
    ORDER BY 2
    ),
    Customer_sales_rank AS (
    SELECT 
    *,
    rank() OVER (PARTITION BY Quarter, Year ORDER BY Total DESC) as CustomerRank
    FROM Customer_sales
    ) 
    SELECT * FROM Customer_sales_rank WHERE CustomerRank in (1,2,3,4,5) ORDER BY Year
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex10(conn):
    
    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = """
    WITH Month_rank AS (
    SELECT 
    CASE
        WHEN strftime('%m', o.OrderDate) = '01' THEN 'January'
        WHEN strftime('%m', o.OrderDate) = '02' THEN 'February'
        WHEN strftime('%m', o.OrderDate) = '03' THEN 'March'
        WHEN strftime('%m', o.OrderDate) = '04' THEN 'April'
        WHEN strftime('%m', o.OrderDate) = '05' THEN 'May'
        WHEN strftime('%m', o.OrderDate) = '06' THEN 'June'
        WHEN strftime('%m', o.OrderDate) = '07' THEN 'July'
        WHEN strftime('%m', o.OrderDate) = '08' THEN 'August'
        WHEN strftime('%m', o.OrderDate) = '09' THEN 'September'
        WHEN strftime('%m', o.OrderDate) = '10' THEN 'October'
        WHEN strftime('%m', o.OrderDate) = '11' THEN 'November'
        WHEN strftime('%m', o.OrderDate) = '12' THEN 'December'
    END Month, 
    Sum(ROUND(p.ProductUnitPrice * o.QuantityOrdered)) as Total
    FROM OrderDetail o
    JOIN Product p on o.ProductID = p.ProductID
    GROUP BY 1
    )  
    SELECT 
    Month,
    Total,
    rank() OVER (ORDER BY Total DESC) as TotalRank
    FROM Month_rank 
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION

    sql_statement = """
    WITH orderdate AS (
        SELECT 
        CustomerID,
        OrderDate,
        LAG(OrderDate,1) OVER (PARTITION BY CustomerID ORDER BY OrderDate ASC) as PreviousOrderDate
        FROM OrderDetail
    ), maxdays AS (
        SELECT 
        CustomerID,
        OrderDate,
        PreviousOrderDate,
        Max(julianday(OrderDate) - julianday(PreviousOrderDate)) as MaxDaysWithoutOrder
        FROM orderdate
        GROUP BY 1
        ORDER BY 4 DESC
    )
    SELECT
    m.CustomerID,
    c.FirstName,
    c.LastName,
    ct.Country,
    m.OrderDate,
    m.PreviousOrderDate,
    m.MaxDaysWithoutOrder
    FROM maxdays m
    JOIN Customer c on m.CustomerID = c.CustomerID
    JOIN Country ct on c.CountryID = ct.CountryID
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement
LOAD DATA INFILE '/opt/lampp/var/mysql/sales_data.csv'
INTO TABLE sales_data
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;


-- Insert into brand_sales
INSERT INTO brand_sales (brandname, nooforders, sales, aov, orderdate)
SELECT 
    brandname,  
    COUNT(DISTINCT invoice) AS nooforders, 
    SUM(orderamountnet) AS sales, 
    ROUND(SUM(orderamountnet) / NULLIF(COUNT(DISTINCT invoice), 0), 2) AS aov,
    orderdate::DATE
FROM sales_data
WHERE orderdate = '2025-03-27'::DATE
GROUP BY brandname, orderdate;

-- Insert into category_sales
INSERT INTO category_sales (subcategoryof, sales, orderdate)
SELECT 
    subcategoryof, 
    SUM(orderamountnet) AS sales,
    orderdate::DATE
FROM sales_data
WHERE orderdate = '2025-03-27'::DATE
GROUP BY subcategoryof, orderdate;

-- Insert into product_sales
INSERT INTO product_sales (productname, nooforders, sales, quantitysold, orderdate)
SELECT 
    productname,  
    COUNT(DISTINCT invoice) AS nooforders, 
    SUM(orderamountnet) AS sales, 
    SUM(quantity) AS quantitysold,
    orderdate::DATE
FROM sales_data
WHERE orderdate = '2025-03-27'::DATE
GROUP BY productname, orderdate;

-- Insert into store_sales
INSERT INTO store_sales (storename, nooforder, sales, aov, orderdate)
SELECT 
    storename, 
    COUNT(DISTINCT invoice) AS nooforder,
    SUM(orderamountnet) AS sales, 
    ROUND(SUM(orderamountnet) / NULLIF(COUNT(DISTINCT invoice), 0), 2) AS aov, 
    orderdate::DATE
FROM sales_data
WHERE orderdate = '2025-03-27'::DATE
GROUP BY storename, orderdate;

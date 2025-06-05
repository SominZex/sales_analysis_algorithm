-- Linux
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
    SUM(totalProductPrice) AS sales, 
    ROUND(SUM(totalProductPrice) / NULLIF(COUNT(DISTINCT invoice), 0), 2) AS aov,
    orderdate
FROM sales_data
WHERE orderdate = '2025-05-03'
GROUP BY brandname, orderdate;

-- Insert into category_sales
INSERT INTO category_sales (subcategoryof, sales, orderdate)
SELECT 
    subcategoryof, 
    SUM(totalProductPrice) AS sales,
    orderdate
FROM sales_data
WHERE orderdate = '2025-05-03'
GROUP BY subcategoryof, orderdate;

-- Insert into product_sales
INSERT INTO product_sales (productname, nooforders, sales, quantitysold, orderdate)
SELECT 
    productname,  
    COUNT(DISTINCT invoice) AS nooforders, 
    SUM(totalProductPrice) AS sales, 
    SUM(quantity) AS quantitysold,
    orderdate
FROM sales_data
WHERE orderdate = '2025-05-03'
GROUP BY productname, orderdate;

-- Insert into store_sales
INSERT INTO store_sales (storename, nooforder, sales, aov, orderdate)
SELECT 
    storename, 
    COUNT(DISTINCT invoice) AS nooforder,
    SUM(totalProductPrice) AS sales, 
    ROUND(SUM(totalProductPrice) / NULLIF(COUNT(DISTINCT invoice), 0), 2) AS aov, 
    orderdate
FROM sales_data
WHERE orderdate = '2025-05-03'
GROUP BY storename, orderdate;


UPDATE sales_data
SET productName = 'Cadbury Dairy Milk Silk Fruit & Nut Chocolate 55g'
WHERE productName = 'Cadbury Dairy Milk Silk Fruit & Nut Chocolate Bar 55g';

UPDATE sales_data
SET productName = 'Ocean Peach & Passion Fruit Fruit Drink 500 ml'
WHERE productName = 'Ocean Peach & Passion Fruit Flavour Fruit Drink 500 ml';

UPDATE sales_data
SET productName = "Kwality Wall's Magnum Chocolate Ice Cream 70ml"
WHERE productName = "Kwality Wall's Magnum Chocolate Truffle Ice Cream 70ml";

UPDATE sales_data
SET productName = "Cadbury Dairy Milk Silk Fruit & Nut Bar 137g"
WHERE productName = "Cadbury Dairy Milk Silk Fruit & Nut Chocolate Bar 137g";


UPDATE sales_data
SET productName = "Nutella & Go Hazelnut Spread & Pretzels 48gm"
WHERE productName = "Nutella & Go Hazelnut Spread & Pretzels Sticks, 48gm";


UPDATE sales_data
SET productName = "Coca Cola Diet Coke Carbonated Soft Drink 300ml"
WHERE productName = "Coca Cola Diet Coke Carbonated Soft Drink Can 300ml";


LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\may26_27.csv'
INTO TABLE sales_data.sales_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
  invoice, storeInvoice, orderDate, time, productId, productName, barcode, quantity, sellingPrice, discountAmount,
  totalProductPrice, deliveryFee, HSNCode, GST, GSTAmount, CGSTRate, CGSTAmount, SGSTRate, SGSTAmount,
  acessAmount, cess, cessAmount, orderAmountTax, orderAmountNet, cashAmount, cardAmount, upiAmount, creditAmount,
  costPrice, description, brandName, categoryName, subCategoryOf, storeName, GSTIN, orderType, paymentMethod,
  customerName, customerNumber, orderFrom, orderStatus
);
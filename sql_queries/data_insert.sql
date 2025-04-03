INSERT INTO brand_sales (brandName, NoOfOrders, Sales, AOV, orderDate)
SELECT 
    brandName,  
    COUNT(DISTINCT invoice) AS NoOfOrders, 
    SUM(orderAmountNet) AS Sales, 
    ROUND(SUM(orderAmountNet) / NULLIF(COUNT(DISTINCT invoice), 0), 2) AS AOV,
    orderDate
FROM sales_data
WHERE orderDate = '2025-03-16'
GROUP BY brandName, orderDate
ORDER BY Sales DESC;

-- Insert into category_sales
INSERT INTO category_sales (subCategoryOf, Sales, orderDate)
SELECT 
    subCategoryOf, 
    SUM(orderAmountNet) AS Sales,
    orderDate
FROM sales_data
WHERE orderDate = '2025-03-16'
GROUP BY subCategoryOf, orderDate
ORDER BY Sales DESC;

-- Insert into product_sales
INSERT INTO product_sales (productName, NoOfOrders, Sales, QuantitySold, orderDate)
SELECT 
    productName,  
    COUNT(DISTINCT invoice) AS NoOfOrders, 
    SUM(orderAmountNet) AS Sales, 
    SUM(quantity) AS QuantitySold,
    orderDate
FROM sales_data
WHERE orderDate = '2025-03-16'
GROUP BY productName, orderDate
ORDER BY Sales DESC;

-- Insert into store_sales
INSERT INTO store_sales (storeName, NoOfOrder, sales, AOV, orderDate)
SELECT 
    storeName, 
    COUNT(DISTINCT invoice) AS NoOfOrders,
    SUM(orderAmountNet) AS sales, 
    ROUND(SUM(orderAmountNet) / NULLIF(COUNT(DISTINCT invoice), 0), 2) AS AOV, 
    orderDate
FROM sales_data
WHERE orderDate = '2025-03-16'
GROUP BY storeName, orderDate
ORDER BY sales DESC;
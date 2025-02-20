INSERT INTO product_sales (productName, NoOfOrders, Sales, QuantitySold, orderDate)
SELECT 
    productName,  
    COUNT(DISTINCT invoice) AS NoOfOrders, 
    SUM(orderAmountNet) AS Sales, 
    SUM(quantity) AS QuantitySold,
    orderDate
FROM sales_data
GROUP BY productName, orderDate
ORDER BY orderDate DESC, Sales DESC;

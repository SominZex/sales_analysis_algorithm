INSERT INTO store_sales (storeName, NoOfOrders, sales, AOV, orderDate)
SELECT 
    storeName, 
    COUNT(DISTINCT invoice) AS `NoofOrders`,
    SUM(orderAmountNet) AS `sales`, 
    ROUND(SUM(orderAmountNet) / NULLIF(COUNT(DISTINCT invoice), 0), 2) AS `AOV`, 
    orderDate
FROM sales_data
GROUP BY storeName, orderDate
ORDER BY orderDate DESC, `sales` DESC;

INSERT INTO brand_sales (brandName, NoOfOrders, Sales, AOV, orderDate)
SELECT 
    brandName,  
    COUNT(DISTINCT invoice) AS NoOfOrders, 
    SUM(orderAmountNet) AS Sales, 
    ROUND(SUM(orderAmountNet) / NULLIF(COUNT(DISTINCT invoice), 0), 2) AS AOV,
    orderDate
FROM sales_data
WHERE orderDate BETWEEN '2025-02-05' AND '2025-02-07' 
GROUP BY brandName, orderDate
ORDER BY orderDate DESC, Sales DESC;

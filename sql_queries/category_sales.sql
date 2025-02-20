INSERT INTO category_sales (subCategoryOf, Sales, orderDate)
SELECT 
    subCategoryOf, 
    SUM(orderAmountNet) AS `Sales`,
    orderDate
FROM sales_data
GROUP BY subCategoryOf, orderDate
ORDER BY orderDate DESC, `Sales` DESC;

INSERT INTO billing_data (
    "invoice", "storeInvoice", "orderDate", "time", "productId", "productName", "barcode",
    "quantity", "sellingPrice", "discountAmount", "totalProductPrice", "deliveryFee",
    "HSNCode", "GST", "GSTAmount", "CGSTRate", "CGSTAmount", "SGSTRate", "SGSTAmount",
    "acessAmount", "cess", "cessAmount", "orderAmountTax", "orderAmountNet", "cashAmount",
    "cardAmount", "upiAmount", "creditAmount", "costPrice", "description", "brandName",
    "categoryName", "subCategoryOf", "storeName", "GSTIN", "orderType", "paymentMethod",
    "customerName", "customerNumber", "orderFrom", "orderStatus"
)

SELECT
    "invoice", "storeInvoice", "orderDate", "time", "productId", "productName", "barcode",
    "quantity", 
    CAST("sellingPrice" AS numeric), 
    CAST("discountAmount" AS numeric), 
    CAST("totalProductPrice" AS numeric), 
    CAST("deliveryFee" AS numeric),
    "HSNCode", 
    CAST("GST" AS numeric), 
    CAST("GSTAmount" AS numeric), 
    CAST("CGSTRate" AS numeric), 
    CAST("CGSTAmount" AS numeric), 
    CAST("SGSTRate" AS numeric), 
    CAST("SGSTAmount" AS numeric),
    CAST("acessAmount" AS numeric), 
    CAST("cess" AS numeric), 
    CAST("cessAmount" AS numeric), 
    CAST("orderAmountTax" AS numeric), 
    CAST("orderAmountNet" AS numeric), 
    CAST("cashAmount" AS numeric),
    CAST("cardAmount" AS numeric), 
    CAST("upiAmount" AS numeric),
    CAST("creditAmount" AS numeric), 
    CAST("costPrice" AS numeric),
    "description", "brandName", "categoryName", "subCategoryOf", "storeName", "GSTIN",
    "orderType", "paymentMethod", "customerName", "customerNumber", "orderFrom", "orderStatus"
FROM sales_data;


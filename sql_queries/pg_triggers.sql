CREATE OR REPLACE FUNCTION insert_brand_sales()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO brand_sales (brandname, nooforders, sales, aov, orderdate)
    SELECT 
        NEW."brandName",
        COUNT(DISTINCT "invoice"),
        SUM("totalProductPrice"),
        ROUND(SUM("totalProductPrice") / NULLIF(COUNT(DISTINCT "invoice"), 0), 2),
        NEW."orderDate"
    FROM billing_data
    WHERE "brandName" = NEW."brandName"
      AND "orderDate" = NEW."orderDate"
    GROUP BY "brandName", "orderDate";

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_insert_brand_sales
AFTER INSERT ON billing_data
FOR EACH ROW
EXECUTE FUNCTION insert_brand_sales();


CREATE OR REPLACE FUNCTION insert_category_sales()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO category_sales (subcategoryof, sales, orderdate)
    SELECT 
        NEW."subCategoryOf",
        SUM("totalProductPrice"),
        NEW."orderDate"
    FROM billing_data
    WHERE "subCategoryOf" = NEW."subCategoryOf"
      AND "orderDate" = NEW."orderDate"
    GROUP BY "subCategoryOf", "orderDate";

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_insert_category_sales
AFTER INSERT ON billing_data
FOR EACH ROW
EXECUTE FUNCTION insert_category_sales();


CREATE OR REPLACE FUNCTION insert_product_sales()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO product_sales (productname, nooforders, sales, quantitysold, orderdate)
    SELECT 
        NEW."productName",
        COUNT(DISTINCT "invoice"),
        SUM("totalProductPrice"),
        SUM("quantity"),
        NEW."orderDate"
    FROM billing_data
    WHERE "productName" = NEW."productName"
      AND "orderDate" = NEW."orderDate"
    GROUP BY "productName", "orderDate";

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_insert_product_sales
AFTER INSERT ON billing_data
FOR EACH ROW
EXECUTE FUNCTION insert_product_sales();


CREATE OR REPLACE FUNCTION insert_store_sales()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO store_sales (storename, nooforder, sales, aov, orderdate)
    SELECT 
        NEW."storeName",
        COUNT(DISTINCT "invoice"),
        SUM("totalProductPrice"),
        ROUND(SUM("totalProductPrice") / NULLIF(COUNT(DISTINCT "invoice"), 0), 2),
        NEW."orderDate"
    FROM billing_data
    WHERE "storeName" = NEW."storeName"
      AND "orderDate" = NEW."orderDate"
    GROUP BY "storeName", "orderDate";

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_insert_store_sales
AFTER INSERT ON billing_data
FOR EACH ROW
EXECUTE FUNCTION insert_store_sales();

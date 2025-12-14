CREATE TABLE IF NOT EXISTS sales_by_products (
    name String,
    category String,
    total_income Float64,
    total_quantity Int64,
    avg_rating Float64,
    review_count Int64
) ENGINE = MergeTree()
ORDER BY (name);

CREATE TABLE IF NOT EXISTS sales_by_customers (
    first_name String,
    last_name String,
    country String,
    total_purchases Float64,
    orders_count Int64,
    avg_sale_price Float64
) ENGINE = MergeTree()
ORDER BY (first_name, last_name);

CREATE TABLE IF NOT EXISTS sales_by_time (
    year Int32,
    month Int32,
    monthly_income Float64,
    orders_count Int64,
    avg_sale_price Float64
) ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS sales_by_stores (
    name String,
    city String,
    country String,
    total_income Float64,
    orders_count Int64,
    avg_sale_price Float64
) ENGINE = MergeTree()
ORDER BY (name);

CREATE TABLE IF NOT EXISTS sales_by_suppliers (
    name String,
    country String,
    total_income Float64,
    avg_price Float64
) ENGINE = MergeTree()
ORDER BY (name);

CREATE TABLE IF NOT EXISTS product_quality (
    name String,
    avg_rating Float64,
    review_count Int64,
    sales_count Int64,
    rating_sales_correlation Float64
) ENGINE = MergeTree()
ORDER BY (name);
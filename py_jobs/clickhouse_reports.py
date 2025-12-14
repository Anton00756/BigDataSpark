from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum, count, countDistinct, corr, broadcast


spark = SparkSession.builder.appName("reports_job").getOrCreate()
jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
prop = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

jdbc_url_clickhouse = "jdbc:clickhouse:http://clickhouse:8123/default?ssl=false&compress=0"
prop_clickhouse = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "custom_user",
    "password": "custom_password",
    "socket_timeout": "300000",
    "connect_timeout": "30000",
    "batchsize": "1000"
}

fact = spark.read.jdbc(jdbc_url, "fact_sales", properties=prop).repartition(50).alias("fact").persist()
dim_customer = spark.read.jdbc(jdbc_url, "dim_customer", properties=prop).repartition(50).alias("dim_customer").persist()
dim_seller = spark.read.jdbc(jdbc_url, "dim_seller", properties=prop).repartition(50).alias("dim_seller").persist()
dim_product = spark.read.jdbc(jdbc_url, "dim_product", properties=prop).repartition(50).alias("dim_product").persist()
dim_supplier = spark.read.jdbc(jdbc_url, "dim_supplier", properties=prop).repartition(50).alias("dim_supplier").persist()
dim_store = spark.read.jdbc(jdbc_url, "dim_store", properties=prop).repartition(50).alias("dim_store").persist()
dim_date = spark.read.jdbc(jdbc_url, "dim_date", properties=prop).repartition(50).alias("dim_date").persist()

dim_customer_broadcast = broadcast(dim_customer)
dim_seller_broadcast = broadcast(dim_seller)
dim_product_broadcast = broadcast(dim_product)
dim_supplier_broadcast = broadcast(dim_supplier)
dim_store_broadcast = broadcast(dim_store)
dim_date_broadcast = broadcast(dim_date)

sales_by_products = fact.join(dim_product_broadcast, col("fact.product_sk") == col("dim_product.product_sk")) \
    .groupBy(col("dim_product.name"), col("dim_product.category")) \
    .agg(sum(col("fact.sale_total_price")).alias("total_income"),
         sum(col("fact.sale_quantity")).alias("total_quantity"),
         avg(col("dim_product.rating")).alias("avg_rating"),
         count(when(col("dim_product.rating").isNotNull(), 1)).alias("review_count"))
sales_by_products.write.jdbc(jdbc_url_clickhouse, "sales_by_products", mode="append", properties=prop_clickhouse)

sales_by_customers = fact.join(dim_customer_broadcast, col("fact.customer_sk") == col("dim_customer.customer_sk")) \
    .groupBy(col("dim_customer.first_name"), col("dim_customer.last_name"), col("dim_customer.country")) \
    .agg(sum(col("fact.sale_total_price")).alias("total_purchases"),
         count("*").alias("orders_count"),
         avg(col("fact.sale_total_price")).alias("avg_sale_price"))
sales_by_customers.write.jdbc(jdbc_url_clickhouse, "sales_by_customers", mode="append", properties=prop_clickhouse)

sales_by_time = fact.join(dim_date_broadcast, col("fact.date_sk") == col("dim_date.date_sk")) \
    .groupBy(col("dim_date.year"), col("dim_date.month")) \
    .agg(sum(col("fact.sale_total_price")).alias("monthly_income"),
         count("*").alias("orders_count"),
         avg(col("fact.sale_total_price")).alias("avg_sale_price"))
sales_by_time.write.jdbc(jdbc_url_clickhouse, "sales_by_time", mode="append", properties=prop_clickhouse)

sales_by_stores = fact.join(dim_store_broadcast, col("fact.store_sk") == col("dim_store.store_sk")) \
    .groupBy(col("dim_store.name"), col("dim_store.city"), col("dim_store.country")) \
    .agg(sum(col("fact.sale_total_price")).alias("total_income"),
         count("*").alias("orders_count"),
         avg(col("fact.sale_total_price")).alias("avg_sale_price"))
sales_by_stores.write.jdbc(jdbc_url_clickhouse, "sales_by_stores", mode="append", properties=prop_clickhouse)

sales_by_suppliers = fact.join(dim_supplier_broadcast, col("fact.supplier_sk") == col("dim_supplier.supplier_sk")) \
    .groupBy(col("dim_supplier.name"), col("dim_supplier.country")) \
    .agg(sum(col("fact.sale_total_price")).alias("total_income"),
         (sum(col("fact.sale_total_price")) / sum(col("fact.sale_quantity"))).alias("avg_price"))
sales_by_suppliers.write.jdbc(jdbc_url_clickhouse, "sales_by_suppliers", mode="append", properties=prop_clickhouse)

product_quality = fact.join(dim_product_broadcast, col("fact.product_sk") == col("dim_product.product_sk")) \
    .groupBy(col("dim_product.name")) \
    .agg(avg(col("dim_product.rating")).alias("avg_rating"),
         count(when(col("dim_product.rating").isNotNull(), 1)).alias("review_count"),
         sum(col("fact.sale_quantity")).alias("sales_count"),
         corr(col("dim_product.rating"), col("fact.sale_quantity")).alias("rating_sales_correlation"))
product_quality.write.jdbc(jdbc_url_clickhouse, "product_quality", mode="append", properties=prop_clickhouse)

fact.unpersist()
dim_customer.unpersist()
dim_seller.unpersist()
dim_product.unpersist()
dim_supplier.unpersist()
dim_store.unpersist()

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, monotonically_increasing_id, year, month, dayofmonth


spark = SparkSession.builder.appName("star_job").getOrCreate()
jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
prop = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

raw = (
    spark.read.jdbc(jdbc_url, "mock_data", properties=prop)
    .withColumn("sale_date", col("sale_date").cast("date"))
    .withColumn("product_price", col("product_price").cast("double"))
    .withColumn("sale_quantity", col("sale_quantity").cast("int"))
    .withColumn(
        "sale_total_price",
        coalesce(
            col("sale_total_price").cast("double"),
            col("product_price") * col("sale_quantity")
        )
    )
)

dim_customer = (
    raw
    .filter(col("sale_customer_id").isNotNull())
    .select(
        col("sale_customer_id").alias("customer_id"),
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code")
    )
    .dropDuplicates(["customer_id"])
    .withColumn("customer_sk", monotonically_increasing_id())
)

dim_customer.write.jdbc(
    jdbc_url, "dim_customer", mode="overwrite", properties=prop
)

dim_product = (
    raw
    .filter(col("sale_product_id").isNotNull())
    .select(
        col("sale_product_id").alias("product_id"),
        col("product_name").alias("name"),
        col("product_category").alias("category"),
        col("product_weight").alias("weight"),
        col("product_color").alias("color"),
        col("product_size").alias("size"),
        col("product_brand").alias("brand"),
        col("product_material").alias("material"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        col("product_release_date").alias("release_date"),
        col("product_expiry_date").alias("expiry_date"),
        col("product_price").alias("price")
    )
    .dropDuplicates(["product_id"])
    .withColumn("product_sk", monotonically_increasing_id())
)

dim_product.write.jdbc(
    jdbc_url, "dim_product", mode="overwrite", properties=prop
)

dim_seller = (
    raw
    .filter(col("sale_seller_id").isNotNull())
    .select(
        col("sale_seller_id").alias("seller_id"),
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code")
    )
    .dropDuplicates(["seller_id"])
    .withColumn("seller_sk", monotonically_increasing_id())
)

dim_seller.write.jdbc(
    jdbc_url, "dim_seller", mode="overwrite", properties=prop
)

dim_store = (
    raw
    .filter(col("store_name").isNotNull())
    .select(
        col("store_name").alias("name"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_country").alias("country"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email")
    )
    .dropDuplicates(["name"])
    .withColumn("store_sk", monotonically_increasing_id())
)

dim_store.write.jdbc(
    jdbc_url, "dim_store", mode="overwrite", properties=prop
)

dim_supplier = (
    raw
    .filter(col("supplier_name").isNotNull())
    .select(
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("supplier_country").alias("country")
    )
    .dropDuplicates(["name"])
    .withColumn("supplier_sk", monotonically_increasing_id())
)

dim_supplier.write.jdbc(
    jdbc_url, "dim_supplier", mode="overwrite", properties=prop
)

dim_date = raw.select("sale_date").distinct() \
    .withColumn("year", year("sale_date")) \
    .withColumn("month", month("sale_date")) \
    .withColumn("day", dayofmonth("sale_date")) \
    .withColumn("date_sk", monotonically_increasing_id())

dim_date.write.jdbc(
    jdbc_url, "dim_date", mode="overwrite", properties=prop
)

fact_sales = (
    raw.alias("mock")
    .filter(
        col("mock.sale_customer_id").isNotNull() &
        col("mock.sale_seller_id").isNotNull() &
        col("mock.sale_product_id").isNotNull()
    )
    .join(
        dim_customer.alias("d_c"),
        col("mock.sale_customer_id") == col("d_c.customer_id"),
        "inner"
    )
    .join(
        dim_seller.alias("d_s"),
        col("mock.sale_seller_id") == col("d_s.seller_id"),
        "inner"
    )
    .join(
        dim_product.alias("d_p"),
        col("mock.sale_product_id") == col("d_p.product_id"),
        "inner"
    )
    .join(
        dim_store.alias("d_st"),
        col("mock.store_name") == col("d_st.name"),
        "inner"
    )
    .join(
        dim_supplier.alias("d_sup"),
        col("mock.supplier_name") == col("d_sup.name"),
        "inner"
    )
    .join(
        dim_date.alias("d_d"),
        col("mock.sale_date") == col("d_d.sale_date"),
        "inner"
    )
    .select(
        col("d_c.customer_sk"),
        col("d_s.seller_sk"),
        col("d_p.product_sk"),
        col("d_st.store_sk"),
        col("d_sup.supplier_sk"),
        col("d_d.date_sk"),
        col("mock.sale_quantity"),
        col("mock.sale_total_price")
    )
)
fact_sales.write.jdbc(jdbc_url, "fact_sales", mode="overwrite", properties=prop)

spark.stop()

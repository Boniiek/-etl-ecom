package ecom.tech.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;
@Slf4j
public class SparkETL {
    private final SparkSession spark;
    private final String bucket;
    private final String endpoint;

    public SparkETL(SparkSession spark, String bucket, String endpoint) {
        this.spark = spark;
        this.bucket = bucket;
        this.endpoint = endpoint;
    }
    private Dataset<Row> readParquetFolder(String folderName) {
        String path = "s3a://" + bucket + "/" + folderName + "/";

        try {
            Dataset<Row> df = spark.read().parquet(path);
            long count = df.count();
            log.info("Loaded {} records from {}/", count, folderName);
            return df;
        } catch (Exception e) {
            log.warn("No data found in {}/ - {}", folderName, e.getMessage());
            return spark.emptyDataFrame();
        }
    }

    public void run() {
        log.info("ETL process started");
        log.info("Bucket: {}", bucket);

        try {
            Dataset<Row> stores = readParquetFolder("store");
            Dataset<Row> users = readParquetFolder("user");
            Dataset<Row> orders = readParquetFolder("order");

            if (stores.isEmpty() || users.isEmpty() || orders.isEmpty()) {
                log.error("Missing critical data - Store:{}, User:{}, Order:{}",
                        !stores.isEmpty(), !users.isEmpty(), !orders.isEmpty());
                spark.close();
                return;
            }

            Dataset<Row> users2025 = users
                    .filter(year(col("created_at")).equalTo(2025))
                    .select(col("id").alias("user_id"));

            log.info("Users from 2025: {}", users2025.count());

            if (users2025.isEmpty()) {
                log.warn("No users from 2025 found - ETL stopped");
                spark.close();
                return;
            }

            Dataset<Row> validOrders = orders
                    .filter(col("status").isin("completed", "delivered", "paid"))
                    .join(users2025, orders.col("user_id").equalTo(users2025.col("user_id")), "inner")
                    .select(orders.col("store_id"), orders.col("amount"));

            log.info("Valid orders from 2025 users: {}", validOrders.count());

            if (validOrders.isEmpty()) {
                log.warn("No valid orders from 2025 users");
                spark.close();
                return;
            }

            Dataset<Row> storeAmounts = validOrders
                    .groupBy("store_id")
                    .agg(sum("amount").alias("target_amount"));

            Dataset<Row> result = storeAmounts
                    .join(stores, storeAmounts.col("store_id").equalTo(stores.col("id")), "inner")
                    .select(
                            stores.col("city"),
                            stores.col("name").alias("store_name"),
                            col("target_amount")
                    );

            WindowSpec windowSpec = Window
                    .partitionBy(col("city"))
                    .orderBy(col("target_amount").desc());

            Dataset<Row> topStores = result
                    .withColumn("rank", row_number().over(windowSpec))
                    .filter(col("rank").leq(3))
                    .drop("rank")
                    .orderBy(col("city"), col("target_amount").desc());

            long resultCount = topStores.count();
            log.info("Top 3 stores per city: {} records", resultCount);

            if (resultCount > 0) {
                String outputPath = "s3a://" + bucket + "/result/";

                topStores.coalesce(1)
                        .write()
                        .mode(SaveMode.Overwrite)
                        .option("header", "true")
                        .csv(outputPath);

                log.info("Result saved to: {}", outputPath);
            } else {
                log.warn("No results to save");
            }

            log.info("ETL process completed successfully");

        } catch (Exception e) {
            log.error("ETL process failed: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            spark.close();
        }
    }
}

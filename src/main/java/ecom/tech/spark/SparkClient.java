package ecom.tech.spark;

import org.apache.spark.sql.SparkSession;

public class SparkClient {

    public static SparkSession createSparkClient(String endpoint,String accessKey,String secretKey){
        SparkSession spark = SparkSession.builder()
                .appName("StoreAnalyticsETL")
                .master("local[*]")

                .config("spark.hadoop.fs.s3a.endpoint", endpoint)
                .config("spark.hadoop.fs.s3a.access.key", accessKey)
                .config("spark.hadoop.fs.s3a.secret.key", secretKey)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .getOrCreate();
//        spark.sparkContext().setLogLevel("ERROR");
        return spark;
    }
}

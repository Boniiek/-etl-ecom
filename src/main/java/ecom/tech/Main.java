package ecom.tech;

import ecom.tech.s3.S3Initializer;
import ecom.tech.spark.SparkClient;
import ecom.tech.spark.SparkETL;

public class Main {
    public static void main(String[] args) {

        S3Initializer.Initialize();
        SparkETL sparkETL=new SparkETL(SparkClient.createSparkClient(System.getenv("S3_ENDPOINT"),System.getenv("S3_ACCESS_KEY"),System.getenv("S3_SECRET_KEY")),System.getenv("S3_BUCKET_NAME"),System.getenv("S3_ENDPOINT"));
        sparkETL.run();
    }
}
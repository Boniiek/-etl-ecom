package ecom.tech;

import ecom.tech.s3.S3Initializer;
import ecom.tech.spark.SparkClient;
import ecom.tech.spark.SparkETL;

public class Main {
    public static void main(String[] args) {

        S3Initializer.Initialize();
        SparkETL sparkETL=new SparkETL(SparkClient.createSparkClient("http://minio:9000","etl","etladmin123"),"etl-data","http://minio:9000");
        sparkETL.run();
    }
}
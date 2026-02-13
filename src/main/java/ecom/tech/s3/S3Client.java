package ecom.tech.s3;

import io.minio.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3Client {
    private final MinioClient minioClient;
    private String endpoint ;
    private String accessKey;
    private String secretKey;
    private String bucketName;
    public S3Client() {
        System.out.println(System.getenv("S3_ENDPOINT"));
        this.minioClient= MinioClient.builder()
                .endpoint(System.getenv("S3_ENDPOINT"))
                .credentials(System.getenv("S3_ACCESS_KEY"),System.getenv("S3_SECRET_KEY"))
                .build();
        this.bucketName=System.getenv("S3_BUCKET_NAME");
    }
    public void createBucketIfNotExist() {
        log.info("Checking if bucket '{}' exists", bucketName);
        try{
            boolean bucketExists = minioClient.bucketExists(
                    BucketExistsArgs.builder()
                            .bucket(bucketName)
                            .build()
            );
            if(!bucketExists){
                log.info("Bucket '{}' does not exist, creating...", bucketName);
                minioClient.makeBucket(
                        MakeBucketArgs.builder()
                                .bucket(bucketName)
                                .build()
                );
                log.info("Bucket '{}' created successfully", bucketName);
            }else{
                log.info("Bucket '{}' already exists", bucketName);
            }

        }
        catch (Exception e){
            throw new RuntimeException("Failed to create bucket: "+ e.getMessage());
        }
    }

    public void createFolder(String folderName){
        log.info("Creating folder '{}/{}'", bucketName, folderName);
        if(!existFolder(folderName)){
            try{
                log.debug("Folder '{}/{}' does not exist, creating...", bucketName, folderName);
                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucketName)
                                .object(folderName)
                                .stream(new java.io.ByteArrayInputStream(new byte[0]), 0, -1)
                                .build()
                );
                log.info("Folder '{}/{}' created successfully", bucketName, folderName);
            }catch (Exception e){
                throw new RuntimeException("Failed to create folder: "+ folderName +" with exception: "+e.getMessage());
            }
        }
    }

    private boolean existFolder(String folderName){
        log.debug("Checking if folder '{}/{}' exists", bucketName, folderName);
        try{
            minioClient.statObject(
                    StatObjectArgs.builder()
                            .bucket(bucketName)
                            .object(folderName)
                            .build()
            );
            log.debug("Folder '{}/{}' exists", bucketName, folderName);
            return true;
        }catch(Exception e){
            log.debug("Folder '{}/{}' does not exist: {}", bucketName, folderName, e.getMessage());
            return false;
        }
    }
}

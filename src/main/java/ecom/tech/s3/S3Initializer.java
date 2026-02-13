package ecom.tech.s3;

import java.util.Arrays;
import java.util.List;

public class S3Initializer {

    public static void Initialize(){
        List<String> foldersName =Arrays.asList("store", "order", "user", "result");
        S3Client client=new S3Client();
        client.createBucketIfNotExist();
        foldersName.forEach(folderName->{
            client.createFolder(folderName+"/");
        });
    }
}

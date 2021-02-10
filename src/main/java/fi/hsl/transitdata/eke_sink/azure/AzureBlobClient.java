package fi.hsl.transitdata.eke_sink.azure;


import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import java.io.File;

/**
 * Azure blob client definition
 */
public class AzureBlobClient {
    BlobContainerClient blobContainerClient;

    public AzureBlobClient(String connectionString, String blobContainer) {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
        if (blobServiceClient.getBlobContainerClient(blobContainer).exists())
            this.blobContainerClient = blobServiceClient.getBlobContainerClient(blobContainer);
        else
            this.blobContainerClient = blobServiceClient.createBlobContainer(blobContainer);

    }

    void uploadFromFile(File file) {
        BlobClient blobClient = blobContainerClient.getBlobClient(file.getName());
        blobClient.uploadFromFile(file.getAbsolutePath(), true);

    }

    public boolean fileExists(String filePath) {
        blobContainerClient.getBlobClient(filePath);
        return blobContainerClient.exists();
    }
}


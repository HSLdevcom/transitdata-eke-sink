package fi.hsl.transitdata.eke_sink.azure;

import java.io.File;

/**
 * Uploads file to blob on Azure
 */
public class AzureUploadTask {
    private final AzureBlobClient blobClient;
    private final File file;

    public AzureUploadTask(AzureBlobClient blobClient, File file) {
        this.blobClient = blobClient;
        this.file = file;
    }

    public AzureUploadTask run() {
        //log.info("Uploading dump from filepath: {}", filePath);
        this.blobClient.uploadFromFile(file);
        return this;
    }

    public boolean isUploaded() {
        return this.blobClient.fileExists(file.getAbsolutePath());
    }
}

package fi.hsl.transitdata.eke_sink.azure

import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import mu.KotlinLogging
import java.io.BufferedOutputStream
import java.nio.file.Files
import java.nio.file.Path

class BlobUploader(connectionString: String, container: String) {
    companion object {
        private const val BUFFER_SIZE = 65536
    }

    private val log = KotlinLogging.logger { }

    private val blobServiceClient: BlobServiceClient = BlobServiceClientBuilder().connectionString(connectionString).buildClient()

    private val blobContainerClient: BlobContainerClient = if (blobServiceClient.getBlobContainerClient(container).exists()) {
        blobServiceClient.getBlobContainerClient(container)
    } else {
        blobServiceClient.createBlobContainer(container)
    }

    fun uploadFromFile(path: Path) {
        val blobClient = blobContainerClient.getBlobClient(path.fileName.toString())
        if (blobClient.exists()) {
            log.warn { "Warning! Blob ${blobClient.blobName} already exists and will be overwritten" }
        }

        val outputStream = BufferedOutputStream(blobClient.blockBlobClient.getBlobOutputStream(true), BUFFER_SIZE)
        outputStream.use {
            Files.copy(path, it)
        }
    }
}
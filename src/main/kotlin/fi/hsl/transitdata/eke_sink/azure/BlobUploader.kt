package fi.hsl.transitdata.eke_sink.azure

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import java.io.BufferedOutputStream
import java.nio.file.Files
import java.nio.file.Path
import mu.KotlinLogging

class BlobUploader private constructor(
    private val blobServiceClient: BlobServiceClient,
    private val container: String
) {
    companion object {
        private const val BUFFER_SIZE = 65536

        fun withDefaultAzureCredential(blobAccountName: String, container: String): BlobUploader {
            val client =
                BlobServiceClientBuilder()
                    .endpoint("https://$blobAccountName.blob.core.windows.net")
                    .credential(DefaultAzureCredentialBuilder().build())
                    .buildClient()

            return BlobUploader(client, container)
        }

        fun withConnectionString(connectionString: String, container: String): BlobUploader {
            val client = BlobServiceClientBuilder().connectionString(connectionString).buildClient()

            return BlobUploader(client, container)
        }
    }

    private val log = KotlinLogging.logger {}

    private val blobContainerClient: BlobContainerClient by lazy {
        val containerClient = blobServiceClient.getBlobContainerClient(container)

        if (containerClient.exists()) {
            containerClient
        } else {
            blobServiceClient.createBlobContainer(container)
        }
    }

    fun uploadFromFile(path: Path, tags: Map<String, String>) {
        val blobClient = blobContainerClient.getBlobClient(path.fileName.toString())

        if (blobClient.exists()) {
            log.warn {
                "Warning! Blob ${blobClient.blobName} already exists and will be overwritten"
            }
        }

        val outputStream =
            BufferedOutputStream(blobClient.blockBlobClient.getBlobOutputStream(true), BUFFER_SIZE)

        outputStream.use {
            Files.copy(path, it)
        }

        blobClient.tags = tags
    }
}
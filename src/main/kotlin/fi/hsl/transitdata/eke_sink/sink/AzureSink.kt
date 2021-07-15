package fi.hsl.transitdata.eke_sink.sink

import fi.hsl.transitdata.eke_sink.azure.AzureBlobClient
import fi.hsl.transitdata.eke_sink.azure.AzureUploader
import java.nio.file.Path

/**
 * Sink that uploads data to Azure Blob Storage
 */
class AzureSink(private val azureBlobClient: AzureBlobClient) : Sink {
    override fun upload(file: Path) {
        val uploader = AzureUploader(azureBlobClient)
        uploader.uploadBlob(file.toFile())
    }
}
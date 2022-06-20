package fi.hsl.transitdata.eke_sink.sink

import fi.hsl.transitdata.eke_sink.azure.BlobUploader
import java.nio.file.Path

/**
 * Sink that uploads data to Azure Blob Storage
 */
class AzureSink(private val blobUploader: BlobUploader): Sink {
    override fun upload(file: Path, tags: Map<String, String>) {
        blobUploader.uploadFromFile(file, tags)
    }
}
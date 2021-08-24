package fi.hsl.transitdata.eke_sink

import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.GZIPOutputStream

fun gzip(file: Path): Path {
    val outputFile = file.parent.resolve(file.fileName.toString() + ".gz")
    val outputStream = GZIPOutputStream(Files.newOutputStream(outputFile), 65536)
    outputStream.use { Files.copy(file, it) }
    return outputFile
}
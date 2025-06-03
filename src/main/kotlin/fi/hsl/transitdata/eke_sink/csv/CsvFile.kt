package fi.hsl.transitdata.eke_sink.csv

import mu.KotlinLogging
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.compress.compressors.gzip.GzipParameters
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.io.Closeable
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.Deflater

private val log = KotlinLogging.logger {}

class CsvFile(val path: Path, private val unitNumber: String, csvHeader: List<String>) : Closeable {
    companion object {
        private const val WRITE_BUFFER_SIZE = 65536
        private const val REPLACEMENT_CHAR = "_"
        private val invalidChars = Regex("[^a-zA-Z0-9 +\\-./:=_]")
        fun sanitizeTags(tags: Map<String, String>): Map<String, String> =
            tags.asSequence()
                .mapNotNull { (key, value) ->
                    val sanitizedKey = key.replace(invalidChars, REPLACEMENT_CHAR).take(128)
                    val sanitizedValue = value.replace(invalidChars, REPLACEMENT_CHAR).take(256)
                    if (sanitizedKey.isNotEmpty()) sanitizedKey to sanitizedValue else null
                }
                .toMap()
    }

    private var csvPrinter: CSVPrinter? = CSVPrinter(
        OutputStreamWriter(GzipCompressorOutputStream(Files.newOutputStream(path), GzipParameters().apply {
            compressionLevel = Deflater.BEST_COMPRESSION
            bufferSize = WRITE_BUFFER_SIZE
        }), StandardCharsets.UTF_8),
        CSVFormat.RFC4180.builder().setHeader(*csvHeader.toTypedArray()).build()
    )
    private var open: Boolean = true
    private var lastModified: Long = System.nanoTime()

    private var rowCount = 0
    private var minNtpTime: ZonedDateTime? = null
    private var maxNtpTime: ZonedDateTime? = null

    fun writeRow(csvRow: CsvRow) {
        if (!open) {
            throw IllegalStateException("CsvFile was already closed")
        }

        csvPrinter!!.printRecord(csvRow.asList())

        rowCount++
        if (minNtpTime == null || (csvRow.ntpTime != null && csvRow.ntpTime < minNtpTime)) {
            minNtpTime = csvRow.ntpTime
        }
        if (maxNtpTime == null || (csvRow.ntpTime != null && csvRow.ntpTime > minNtpTime)) {
            maxNtpTime = csvRow.ntpTime
        }

        lastModified = System.nanoTime()
    }

    fun getTags(): Map<String, String> {
        val tags = mutableMapOf<String, String>()
        tags["unit_number"] = unitNumber
        tags["row_count"] = rowCount.toString()
        minNtpTime?.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)?.let { tags["min_ntp_timestamp"] = it }
        maxNtpTime?.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)?.let { tags["max_ntp_timestamp"] = it }
        return CsvFile.sanitizeTags(tags)
    }

    fun getLastModifiedAgo(): Duration = Duration.ofNanos(System.nanoTime() - lastModified)

    override fun close() {
        if (open) {
            csvPrinter?.close(true)
            csvPrinter = null
            Runtime.getRuntime().gc() //Is this needed?
        }
        open = false
    }
}
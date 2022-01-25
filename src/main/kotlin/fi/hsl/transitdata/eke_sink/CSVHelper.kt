package fi.hsl.transitdata.eke_sink

import mu.KotlinLogging
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.compress.compressors.gzip.GzipParameters
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory
import org.apache.commons.pool2.KeyedPooledObjectFactory
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.impl.GenericKeyedObjectPool
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig
import java.io.IOException
import java.io.OutputStreamWriter
import java.io.Writer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.Deflater

/**
 * Helper for writing CSV files. Uses object pool to avoid reopening files for better performance.
 */
class CSVHelper(private val fileDirectory: Path, fileOpenDuration: Duration, private val compress: Boolean, private val csvHeader: List<String>, addToUploadList: (Path) -> Unit) {
    init {
        if (!Files.isDirectory(fileDirectory)) {
            throw IllegalArgumentException("$fileDirectory is not directory")
        }
    }

    private val objectPoolConfig = GenericKeyedObjectPoolConfig<CSVPrinter>().apply {
        minIdlePerKey = 1
        maxIdlePerKey = 1
        maxTotalPerKey = 1

        timeBetweenEvictionRuns = fileOpenDuration.dividedBy(2)
        minEvictableIdleTime = fileOpenDuration //Max time to keep the file open
        numTestsPerEvictionRun = 1000
    }
    private val objectFactory = PooledCSVPrinterFactory(csvHeader, compress, addToUploadList)
    private val objectPool = CSVPrinterPool(objectFactory, objectPoolConfig)

    val openFiles: Int
        get() = objectFactory.openFiles

    /**
     * Writes values to CSV file with specified name
     *
     * @param fileName File name without .csv suffix (e.g. "example" would write to example.csv)
     * @param values List of values. The list must have equal length to CSV header list
     *
     * @return Path of the file where data was written to
     */
    fun writeToCsv(fileName: String, values: List<String>): Path {
        if (values.size != csvHeader.size) {
            throw IllegalArgumentException("List contained different amount of values than CSV header (list: ${values.size}, header: ${csvHeader.size})")
        }

        val fullFileName = if (compress) { "$fileName.csv.gz" } else { "$fileName.csv" }

        val csvFile = fileDirectory.resolve(fullFileName).toAbsolutePath()
        var csvPrinter: CSVPrinter? = null
        try {
            csvPrinter = objectPool.borrowObject(csvFile)
            csvPrinter.printRecord(values)
        } finally {
            if (csvPrinter != null) {
                objectPool.returnObject(csvFile, csvPrinter)
            }
        }
        return csvFile
    }

    private class CSVPrinterPool(factory: KeyedPooledObjectFactory<Path, CSVPrinter>, config: GenericKeyedObjectPoolConfig<CSVPrinter>) : GenericKeyedObjectPool<Path, CSVPrinter>(factory, config)

    private class PooledCSVPrinterFactory(private val csvHeader: List<String>, private val compress: Boolean, private val addToUploadList: (Path) -> Unit) : BaseKeyedPooledObjectFactory<Path, CSVPrinter>() {
        private val log = KotlinLogging.logger {}

        private var _openFiles = AtomicInteger(0)

        val openFiles: Int
            get() = _openFiles.get()

        private fun createFileWriter(path: Path): Writer {
            return if (compress) {
                OutputStreamWriter(GzipCompressorOutputStream(Files.newOutputStream(path), GzipParameters().apply {
                    compressionLevel = Deflater.BEST_COMPRESSION
                    bufferSize = 65536
                }), StandardCharsets.UTF_8)
            } else {
                Files.newBufferedWriter(path, StandardCharsets.UTF_8)
            }
        }

        override fun create(key: Path): CSVPrinter {
            _openFiles.incrementAndGet()
            return CSVPrinter(createFileWriter(key), CSVFormat.RFC4180.withHeader(*csvHeader.toTypedArray()))
        }

        override fun wrap(value: CSVPrinter): PooledObject<CSVPrinter> = DefaultPooledObject(value)

        override fun validateObject(key: Path, p: PooledObject<CSVPrinter>): Boolean = true

        override fun destroyObject(key: Path, p: PooledObject<CSVPrinter>) {
            try {
                log.debug { "Closing CSV file writer for $key" }
                p.`object`.close(true)

                addToUploadList(key)

                _openFiles.decrementAndGet()
            } catch (ioe: IOException) {
                log.error(ioe) { "Failed to close file writer for $key" }
            }
        }
    }
}
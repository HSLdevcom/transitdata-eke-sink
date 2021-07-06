package fi.hsl.transitdata.eke_sink

import mu.KotlinLogging
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory
import org.apache.commons.pool2.KeyedPooledObjectFactory
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.impl.GenericKeyedObjectPool
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

/**
 * Helper for writing CSV files. Uses object pool to avoid reopening files for better performance.
 */
class CSVHelper(private val fileDirectory: Path, fileOpenDuration: Duration, private val csvHeader: List<String>) {
    init {
        if (!Files.isDirectory(fileDirectory)) {
            throw IllegalArgumentException("$fileDirectory is not directory")
        }
    }

    private val objectPoolConfig = GenericKeyedObjectPoolConfig<CSVPrinter>().apply {
        minIdlePerKey = 1
        maxIdlePerKey = 1
        maxTotalPerKey = 1

        timeBetweenEvictionRuns = fileOpenDuration.multipliedBy(2)
        minEvictableIdleTime = fileOpenDuration //Max time to keep the file open
    }
    private val objectPool = CSVPrinterPool(PooledCSVPrinterFactory(csvHeader), objectPoolConfig)

    /**
     * Writes values to CSV file with specified name
     *
     * @param fileName File name without .csv suffix (e.g. "example" would write to example.csv)
     * @param values List of values. The list must have equal length to CSV header list
     */
    fun writeToCsv(fileName: String, values: List<String>) {
        if (values.size != csvHeader.size) {
            throw IllegalArgumentException("List contained different amount of values than CSV header (list: ${values.size}, header: ${csvHeader.size})")
        }

        val csvFile = fileDirectory.resolve("$fileName.csv")
        var csvPrinter: CSVPrinter? = null
        try {
            csvPrinter = objectPool.borrowObject(csvFile)
            csvPrinter.printRecord(values)
        } finally {
            if (csvPrinter != null) {
                objectPool.returnObject(csvFile, csvPrinter)
            }
        }
    }


    private class CSVPrinterPool(factory: KeyedPooledObjectFactory<Path, CSVPrinter>, config: GenericKeyedObjectPoolConfig<CSVPrinter>) : GenericKeyedObjectPool<Path, CSVPrinter>(factory, config)

    private class PooledCSVPrinterFactory(private val csvHeader: List<String>) : BaseKeyedPooledObjectFactory<Path, CSVPrinter>() {
        private val log = KotlinLogging.logger {}

        override fun create(key: Path): CSVPrinter = CSVPrinter(Files.newBufferedWriter(key, StandardCharsets.UTF_8), CSVFormat.RFC4180.withHeader(*csvHeader.toTypedArray()))

        override fun wrap(value: CSVPrinter): PooledObject<CSVPrinter> = DefaultPooledObject(value)

        override fun validateObject(key: Path, p: PooledObject<CSVPrinter>): Boolean = true

        override fun destroyObject(key: Path, p: PooledObject<CSVPrinter>) {
            try {
                log.debug { "Closing CSV file writer for $key" }
                p.`object`.close()
            } catch (ioe: IOException) {
                log.error(ioe) { "Failed to close file writer for $key" }
            }
        }
    }
}
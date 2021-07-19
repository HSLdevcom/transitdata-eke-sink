package fi.hsl.transitdata.eke_sink.sink

import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Sink that stores data locally. Used for testing.
 *
 * @param delay Delay to use when copying the file. Used for simulating network latency.
 * @param maxFiles Maximum amount of files to store in output directory. -1 for no limit. Oldest files are removed first
 */
@ExperimentalTime
class LocalSink(private val outputDirectory: Path, private val delay: Duration = Duration.ZERO, private val maxFiles: Int = -1) : Sink {
    private val log = KotlinLogging.logger {}

    override fun upload(file: Path) {
        val elapsedTime = measureTime {
            val destination = outputDirectory.resolve(file.fileName)
            log.info { "Copying $file to $destination" }
            Files.copy(file, destination)
        }
        //Sleep for the remaining time
        val sleepTime = maxOf(Duration.ZERO, delay.minus(elapsedTime))
        Thread.sleep(sleepTime.toLongMilliseconds())

        //Remove files if there are more than the maximum amount
        if (maxFiles >= 0) {
            val filesToKeep = Files.list(outputDirectory)
                .sorted(Comparator.comparing(Files::getLastModifiedTime).reversed())
                .limit(maxFiles.toLong())
                .collect(Collectors.toSet())

            Files.list(outputDirectory)
                .filter { path -> path !in filesToKeep }
                .forEach(Files::deleteIfExists)
        }
    }
}
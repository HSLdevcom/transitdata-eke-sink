package fi.hsl.transitdata.eke_sink.sink

import java.nio.file.Path

interface Sink {
    @Throws(Exception::class)
    fun upload(file: Path, tags: Map<String, String>)
}
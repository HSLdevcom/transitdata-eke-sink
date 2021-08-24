package fi.hsl.transitdata.eke_sink.utils

import java.util.concurrent.ThreadFactory

object DaemonThreadFactory : ThreadFactory {
    private var threadCounter = 1L

    override fun newThread(r: Runnable): Thread {
        val thread = Thread(r)
        thread.isDaemon = true
        thread.name = "DaemonThread-${threadCounter++}"
        return thread
    }
}
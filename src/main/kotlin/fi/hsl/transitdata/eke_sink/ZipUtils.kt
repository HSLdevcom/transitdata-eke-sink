package fi.hsl.transitdata.eke_sink

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

public fun zipFiles(path: File, fileName: String, filesToZip: List<String>) : File {
    val zipFile = File(path, fileName)
    val fos = FileOutputStream(zipFile)
    fos.use {
        val zipOut = ZipOutputStream(fos)
        zipOut.use {
            for (srcFile in filesToZip) {
                val fileToZip = File(path, srcFile)
                val fis = FileInputStream(fileToZip)
                fis.use {
                    val zipEntry = ZipEntry(fileToZip.name)
                    zipOut.putNextEntry(zipEntry)
                    val bytes = ByteArray(1024)
                    var length: Int
                    while (fis.read(bytes).also { length = it } >= 0) {
                        zipOut.write(bytes, 0, length)
                    }
                }
            }
        }
    }
    return zipFile
}
package wk.spark.framework.utils

import java.io.{BufferedInputStream, File, FileInputStream, PrintWriter}
import java.util.Properties

/**
  * Created by KE.WANG on 2019/5/16.
  */
object FileUtils extends Serializable {

    private val className = getClass.getSimpleName

    def loadProperties(path: String): Properties = {

        val inputStream = new BufferedInputStream(new FileInputStream(path))
        val p = new Properties
        p.load(inputStream)
        p

    }

    def loadProperties(path: String, prop: Properties): Unit = {
        val file: File = new File(path)
        if (file.isFile && file.getAbsolutePath.endsWith(".properties")) {
            val inputStream = new BufferedInputStream(new FileInputStream(path))
            val p = new Properties
            p.load(inputStream)
            prop.putAll(p)
        } else if (file.isDirectory) {
            file.listFiles().foreach(p => loadProperties(p.getAbsolutePath, prop))
        }
    }

    def checkPath(path: String, filename: String): Boolean = {
        val fileExists: Boolean = isExists(path + "/" + filename)
        val pathExists: Boolean = isExists(path)
        if (!fileExists && pathExists) {
            deleteDir(new File(path + "/" + filename))
            println(s"[$className] Delete " + path + "/" + filename)
        }
        fileExists
    }

    def isExists(path: String): Boolean = {
        new File(path).exists()
    }

    /**
      * 删除目录及其所有内容
      * @param dir 目录
      */
    def deleteDir(dir: File): Unit = {
        val files = dir.listFiles()
        files.foreach(f => {
            if (f.isDirectory) {
                deleteDir(f)
            } else {
                f.delete()
            }
        })
        dir.delete()
    }

    /**
      * 创建目录
      * @param dir 目录路径
      */
    def mkDirs(dir: String): Unit = {
        val file = new File(dir)
        if (!file.exists()) {
            file.mkdirs()
        }
    }

    /**
      * 将内容写入文件
      *
      * @example {{{
      * writeToFile("output.txt"){ writer =>
      *   writer.write("Hello World")
      * }
      * }}}
      *
      * @param filename  文件路径
      * @param codeBlock 写入操作
      */
    def writeToFile(filename: String)(codeBlock: PrintWriter => Unit): Unit = {
        val writer = new PrintWriter(filename)
        try {
            codeBlock(writer)
        } finally {
            writer.flush()
            writer.close()
        }
    }

    def main(args: Array[String]): Unit = {
        val prop = new Properties()
        val path = "D:\\ah-svn\\code20190111\\tls-vehicle-activity\\src\\main\\resources"
        loadProperties(path, prop)
        print(prop)
    }
}

package wk.spark.framework.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.collection.immutable.TreeSet

/**
  * HDFS工具
  *
  */
object HDFSUtils extends Serializable {

    var FS: FileSystem = null

    def init(name: String) = getFS(name)

    /**
      * 获取文件系统
      *
      * @return FileSystem
      */
    private def getFS(name: String): FileSystem = {
        if (FS != null) {
            return FS
        }
        val conf = new Configuration()
        conf.set("fs.defaultFS", name)
        //    conf.set("dfs.replication", "3")
        FS = FileSystem.get(conf)
        FS
    }

    /**
      * 创建文本文件
      *
      * @param path    文本文件路径
      * @param content 文本内容
      */
    def writeText(path: String, content: String): Unit = {
        val fs = getFS
        val buffer = content.getBytes
        fs.create(new Path(path)).write(buffer, 0, buffer.length)
        fs.close()
    }

    /**
      * 上传文件
      *
      * @param src 源文件
      * @param dst 目标
      */
    def upload(src: String, dst: String): Unit = {
        val fs = getFS
        fs.copyFromLocalFile(new Path(src), new Path(dst))
    }

    /**
      * 创建目录
      *
      * @param path 路径
      * @return 是否创建成功
      */
    def mkDir(path: String): Boolean = {
        val fs = getFS
        val res = fs.mkdirs(new Path(path))
        res
    }

    /**
      * 重命名文件
      *
      * @param oldPath 原文件
      * @param newPath 新文件
      */
    def rename(oldPath: String, newPath: String): Unit = {
        val fs = getFS
        fs.rename(new Path(oldPath), new Path(newPath))
    }

    /**
      * 获取文件系统
      *
      * @return FileSystem
      */
    private def getFS: FileSystem = {
        if (FS != null) {
            return FS
        }
        val conf = new Configuration()
        //conf.set("fs.defaultFS", VAContext.VACONT.HDFS_PATH)
        //    conf.set("dfs.replication", "3")
        FS = FileSystem.get(conf)
        FS
    }

    /**
      * 列出路径下的文件
      *
      * @param path 路径
      */
    def listFiles(path: String, recursive: Boolean): Unit = {
        val fs = getFS
        val filesList: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(path), recursive)
        while (filesList.hasNext) {
            val file = filesList.next()
            println(file.getPath.getName)
        }
        println("---------")
        val status = fs.listStatus(new Path(path))
        for (file <- status) {
            val df = if (file.isDirectory) "d" else "f"
            println(file.getPath.getName + " " + df)
        }
    }

    /**
      * 列出路径下的文件
      *
      * @param path 路径
      */
    def listFolders(path: String): TreeSet[String] = {
        val fs = getFS
        var folderSet: TreeSet[String] = TreeSet()
        if (!isExists(path)) {
            return folderSet
        }
        val filesList: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(path), false)
        while (filesList.hasNext) {
            val file = filesList.next()
            println(file.getPath.getName)
        }
        val status = fs.listStatus(new Path(path))
        for (file <- status) {
            if (file.isDirectory) {
                folderSet = folderSet + file.getPath.getName
            }
        }
        folderSet
    }

    /**
      * 检查路径下的某一文件是否存在，如果不存在，就删除路径文件夹
      *
      * @param path     文件路径
      * @param filename 路径下的文件名
      * @return 是否存在
      */
    def checkPath(path: String, filename: String): Boolean = {
        val fileExists = isExists(path + "/" + filename)
        val pathExists = isExists(path)
        if (!fileExists && pathExists) rmFile(path)
        fileExists
    }

    /**
      * 删除文件
      *
      * @param path 路径
      */
    def rmFile(path: String): Boolean = {
        val fs = getFS
        val result = fs.delete(new Path(path), true)
        val pre = "[" + getClass.getSimpleName + "] " + path
        println(pre + (if (result) " Delete Successful" else "Delete Failed"))
        result
    }

    /**
      * 文件是否存在
      *
      * @param path 路径
      * @return 是否存在
      */
    def isExists(path: String): Boolean = {
        val fs = getFS
        val res = fs.exists(new Path(path))
        res
    }

    def isExistsWild(path: String) : Boolean = {
        val fs = getFS
        val status = fs.globStatus(new Path(path))
        status.nonEmpty
    }

    /**
      * 下载文件到本地
      *
      * @param src 数据源路径
      * @param dst 目标路径
      */
    def download(src: String, dst: String): Unit = {
        val fs = getFS
        fs.copyToLocalFile(false, new Path(src), new Path(dst), true)
    }

    def close(): Unit = {
        FS.close()
    }
}

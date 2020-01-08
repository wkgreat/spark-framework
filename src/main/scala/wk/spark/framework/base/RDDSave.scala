package wk.spark.framework.base

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import wk.spark.framework.utils.HDFSUtils

/**
  * Created by KE.WANG on 2019/8/1.
  * RDD保存及缓存特性
  */
trait RDDSave {

    /**
      * 保存RDD至HDFS
      * */
    def saveRDDToHDFS(rdd: RDD[String], path: String, overwrite:Boolean = true): Unit = {
        if (existRDDData(path)) {
            if(overwrite) {
                rmRDDData(path)
            } else {
                return
            }
        }
        if (!existRDDData(path)) {
            rdd.saveAsTextFile(path)
        }
    }

    /**
      * 保存RDD至HDFS, 延迟计算RDD
      * */
    def calcRDDToHDFS[T](toStringRDDFunc:(RDD[T])=>RDD[String],
                         path: String,
                         overwrite:Boolean = true)(rddFunc: =>RDD[T]): Unit = {
        if (existRDDData(path)) {
            if(overwrite) {
                rmRDDData(path)
            } else {
                return
            }
        }
        if (!existRDDData(path)) {
            toStringRDDFunc(rddFunc).saveAsTextFile(path)
        }
    }

    /**
      * 是否存在RDD数据
      * */
    def existRDDData(path: String): Boolean = {
        if (path == null) {
            return false
        }
        val b = HDFSUtils.isExists(path + "/_SUCCESS")
        if (HDFSUtils.isExists(path) && !b) {
            HDFSUtils.rmFile(path)
            return false
        }
        b
    }

    /**
      * 删除RDD
      * */
    def rmRDDData(path: String): Unit = {
        HDFSUtils.rmFile(path)
    }


    /**
      * 读取RDD 若不存在则存储rdd至HDFS并重新读取 HDFS
      * */
    def readOrSaveToHDFS[T]( path:String,
                             rdd:RDD[T],
                             toStringRDD:RDD[T]=>RDD[String],
                             fromStringRDD:RDD[String]=>RDD[T],
                             overwrite: Boolean = true ) : RDD[T] = {
        saveRDDToHDFS(toStringRDD(rdd),path,overwrite)
        fromStringRDD(rdd.sparkContext.textFile(path,rdd.getNumPartitions))
    }

    /**
      * 读取RDD 若不存在则存储rdd至HDFS并重新读取 HDFS
      * RDD计算传入一个 传名函数 防止RDD提前action(避免冗余计算)
      * 在path中的RDD数据不存在时，才开始计算RDD并存储
      * */
    def readOrCalcToHDFS[T](sc:SparkContext,
                            path:String,
                            toStringRDD:RDD[T] => RDD[String],
                            fromStringRDD:RDD[String] => RDD[T],
                            overwrite: Boolean = true )(rddFunc: => RDD[T]) : RDD[T] = {
        calcRDDToHDFS[T](toStringRDD,path,overwrite)(rddFunc)
        fromStringRDD(sc.textFile(path,sc.defaultParallelism))
    }

    /**
      * RDD 存储至本地文件系统 供GP入库
      * */
    def saveToLocalDir(ss:SparkSession,
                       rddPath:String,
                       tmpDir:String,
                       localPath:String,
                       partition:Int=1): Unit = {

        val uuid = UUID.randomUUID().toString
        val tmpPath = s"$tmpDir/$uuid"

        if(existRDDData(rddPath)) {
            try {
                println(s"Save $rddPath => $localPath")
                ss.sparkContext.textFile(rddPath).map(s=>s.replaceAll(",","\t")).repartition(partition).saveAsTextFile(tmpPath)
                HDFSUtils.download(tmpPath,localPath)
                println(s"Save $rddPath => $tmpPath => $localPath finished!")
            } catch {
                case e:Exception =>
                    println(s"ERROR!! Save $rddPath => $tmpPath => $localPath error!")
                    e.printStackTrace()
            } finally {
                rmRDDData(tmpPath)
            }
        }
    }

    def show(rdd:RDD[_]): Unit = {
        rdd.take(10).foreach(println)
    }

}

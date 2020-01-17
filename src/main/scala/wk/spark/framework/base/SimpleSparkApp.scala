package wk.spark.framework.base

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import wk.spark.framework.utils.FileUtils

class SimpleSparkApp(confPath:String) extends SparkApp {
    
    private val PROP:Properties = new Properties()
    
    override def initSpark(): SparkSession = {
        FileUtils.loadProperties(confPath, PROP)
        CONF = new SparkConf()
        PROP.keySet().toArray().map(_.toString).filter(_.startsWith("spark."))
            .foreach(k=>CONF.set(k,PROP.getProperty(k)))
        SS = SparkSession.builder().config(CONF).getOrCreate()
        SC = SS.sparkContext
        SC.setLogLevel("WARN")
        SS
    }
    
    override def init(args: Array[String]): Unit = {
        initSpark()
    }
    
    override def start(): Unit = {
    }
    
    override def stop(): Unit = {
        SS.stop()
        println("STOP")
    }
    
    override def main(args: Array[String]): Unit = {
        
        init(args)
        start()
        stop()
        
    }
}

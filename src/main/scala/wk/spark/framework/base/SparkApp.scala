package wk.spark.framework.base

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by KE.WANG on 2019/5/14.
  */
trait SparkApp extends BaseApp {

    private[tls] var CONF: SparkConf = _
    private[tls] var SS: SparkSession = _
    private[tls] var SC: SparkContext = _

    def restore(): Unit = {

        if(SS!=null && SS.isInstanceOf[SparkSession]) {
            SS.stop()
        }
        SS = initSpark()
        SC = SS.sparkContext
        SC.setLogLevel("WARN")
    }

    def initSpark(): SparkSession

}

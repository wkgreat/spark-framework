package wk.spark.framework.base

/**
  * Created by KE.WANG on 2019/5/14.
  */
trait BaseApp extends Serializable {

    def init(args: Array[String]): Unit

    def start(): Unit

    def stop(): Unit

    def main(args: Array[String]): Unit

}

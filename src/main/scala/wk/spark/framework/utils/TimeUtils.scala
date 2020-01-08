package wk.spark.framework.utils

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by KE.WANG on 2018/12/24.
  */
object TimeUtils extends Serializable {

    val DAYSEC = 24 * 3600
    val HOURSEC = 3600

    def yesterDay(day: String): String = {
        second2Str(str2Second(day, "yyyyMMdd") - DAYSEC, "yyyyMMdd")
    }

    def tomorrowDay(day: String): String = {
        second2Str(str2Second(day, "yyyyMMdd") + DAYSEC, "yyyyMMdd")
    }

    def second2Str(seconds: Long, format: String): String = {
        new SimpleDateFormat(format).format(new Date(seconds * 1000L))
    }

    def str2Second(timeStr: String, format: String): Long = {
        new SimpleDateFormat(format).parse(timeStr).getTime / 1000L
    }

    def pastDay(day: String, past: Int): String = {
        second2Str(str2Second(day, "yyyyMMdd") - past * DAYSEC, "yyyyMMdd")
    }

    def lastSeveralDays(day: String, last: Int): Array[String] = {

        val theSecond = str2Second(day, "yyyyMMdd")
        (for (t <- 1 to last) yield theSecond - t * DAYSEC)
            .map(TimeUtils.second2Str(_, "yyyyMMdd")).toArray
    }

    def rangeHour(stimeStr: String, etimeStr: String): Array[String] = {
        val ssec = str2Second(stimeStr,"yyyyMMddHHmmss")
        val esec = str2Second(etimeStr,"yyyyMMddHHmmss")
        (for(s <- ssec to esec by 3600) yield second2Str(s,"yyyyMMddHH")).toArray
    }

    def main(args: Array[String]): Unit = {

        //lastSeveralDays("20190515",5).foreach(println)
        //println(lastDay("20190515"))

    }


}

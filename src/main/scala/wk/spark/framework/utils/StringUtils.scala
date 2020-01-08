package wk.spark.framework.utils

/**
  * Created by KE.WANG on 2018/12/21.
  */
object StringUtils extends Serializable {

    def isBlankString(any: Any): Boolean = {
        val s = any.toString
        var strLen = 0
        if (s == null || s.length == 0) return true
        var i = 0
        for (i <- 0 until s.length) {
            if (!Character.isWhitespace(s.charAt(i))) return false
        }
        true
    }


}

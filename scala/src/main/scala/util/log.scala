package demy.util

object log {
    var showWarnings = true
    var showDebug = false
    def msg(message:Any) {
        val sdfDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
        val now = new java.util.Date();
        val strDate = sdfDate.format(now);
        System.err.println(strDate+"[INFO]---->"+message);
    }
    def warning(message:Any) {
        if(showWarnings) {
          val sdfDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
          val now = new java.util.Date();
          val strDate = sdfDate.format(now);
          System.err.println(strDate+"[WARNING]->"+message);
        }
    }
    def debug(message:Any) {
        if(showDebug) {
          val sdfDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
          val now = new java.util.Date();
          val strDate = sdfDate.format(now);
          System.err.println(strDate+"[DEBUG]--->"+message);
        }
    }
    def error(message:Any) {
      val sdfDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
      val now = new java.util.Date();
      val strDate = sdfDate.format(now);
      System.err.println(strDate+"[ERROR]--->"+message);
    }
}

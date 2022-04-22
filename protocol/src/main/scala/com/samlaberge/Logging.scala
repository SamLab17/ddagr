package com.samlaberge

trait Logging {
  def logDebug(s: String, e: Exception = null): Unit = {
    println(s)
  }

  def logTrace(s: String): Unit = {}

  def log(s: String): Unit = {
    println(s)
  }

  def logErr(s: String, e: Throwable = null): Unit = {
    System.err.println(s);
    if (e != null) {
      System.err.println(e);
      e.printStackTrace(System.err);
    }
  }
}

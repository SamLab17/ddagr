package com.samlaberge

import java.util.Date

trait Logging {

  private def getPrefix: String =
    s"[${this.getClass.getSimpleName}] "

  def logDebug(s: String, e: Exception = null): Unit = {
    println(getPrefix + s)
  }

  def logTrace(s: String): Unit = {}

  def log(s: String): Unit = {
    println(getPrefix + s)
  }

  def logErr(s: String, e: Throwable = null): Unit = {
    System.err.println(getPrefix + s);
    if (e != null) {
      System.err.println(e);
      e.printStackTrace(System.err);
    }
  }
}

package com.samlaberge

object FromSeqApp extends DdagrApp {

  val ddagr = new Ddagr("localhost")

  val data = Seq("hello", "world", "this", "is", "my", "simple", "dataset", "of", "strings")

  val ds = ddagr.from(data)

  println(ds.repartition(2).flatMap(_.iterator).filter(_ != 's').collect())

  ddagr.exit();
}

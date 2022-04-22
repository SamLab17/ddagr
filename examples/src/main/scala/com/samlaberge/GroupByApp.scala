package com.samlaberge

object GroupByApp extends DdagrApp {
  val ddagr = new Ddagr(DdagrOptions("localhost"))

  val ds = ddagr.from(1 to 15).groupBy(_ % 5)

  println(ds.collect())
  println(ds.count())
  println(ds.keys().collect())
  println(ds.mapValues(_.toString + "!").collect())

  println(ds.reduceGroups(_ + _).collect())
}

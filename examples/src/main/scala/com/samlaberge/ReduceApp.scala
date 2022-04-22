package com.samlaberge

object ReduceApp extends DdagrApp {

  val ddagr = new Ddagr(DdagrOptions("localhost"))

  val n = 1000
  val expectedSum = (n * (n+1)) / 2
  val output = ddagr.from(1 to n).repartition(2).reduce(_ + _).collect()




  println(s"Sum of integers 1 to $n")
  println(s"Expected: $expectedSum")
  println(s"Actual: ${output.sum}")
  println(s"All output: ${output}")
}

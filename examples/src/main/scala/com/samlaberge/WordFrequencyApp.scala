package com.samlaberge

object WordFrequencyApp extends DdagrApp {

//  val ddagr = new Ddagr(DdagrOptions("auriga.cs.utexas.edu"))
    val ddagr = new Ddagr(DdagrOptions("localhost"))


    val ds = ddagr.urlTextFile("https://www.gutenberg.org/files/1342/1342-0.txt")
    val wordFrequencies = ds
      .filter(_.nonEmpty)
      .repartition(10)
      .flatMap(_.split("\\s+").map(_.trim).map(_.toLowerCase))
      .filter(w => w.nonEmpty)
      .map(w => (w, 1))
      .groupBy(_._1)
      .mapValues(_._2)
      .reduceGroups(_ + _)
      .firstN(50, (lhs, rhs) => lhs._2 > rhs._2)
      .collect()

    println(wordFrequencies)
    ddagr.exit()

//  val ds = ddagr.from(Seq("hello hello world world hello world sam yo", "here's another line hello", "sam sam"))

}

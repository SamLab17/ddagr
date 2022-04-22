package com.samlaberge

object WordApp extends DdagrApp {


  val ddagr = new Ddagr(DdagrOptions("localhost"))
//      wordLength(ddagr)
  val start = System.nanoTime()
  wordFrequencies(ddagr)
  val nSeconds = (System.nanoTime() - start) / 1e9
  println(s"${"%.2f" format nSeconds} seconds elapsed")
  ddagr.exit()

  def wordLength(ddagr: Ddagr): Unit = {
    val res =
      ddagr.multipleUrlTextFiles(BookUrls.urls)
        .filter(_.nonEmpty)
        .flatMap(line => {
          line.split("\\s+")
            .map(_.replaceAll("\\W", ""))
            .filter(_.nonEmpty)
        })
        .map(_.toLowerCase)
        .groupBy(_.length)
        .mapGroups((len, words) => (len, words.size))
        .collect()
        .sortBy(_._1)
    println(res.mkString("\n"))
  }

  def wordFrequencies(ddagr: Ddagr): Unit = {
    val ds = ddagr.multipleUrlTextFiles(BookUrls.urls)
    val wordFrequencies = ds
      .filter(_.nonEmpty)
      .repartition(10)
      .flatMap(_.split("\\s+").map(_.trim).map(_.toLowerCase))
      .filter(w => w.nonEmpty)
      .groupBy(identity)
      .mapGroups((word, words) => (word, words.size))
      .firstNBy(10, _._2, descending = true)
      .collect()
    println(wordFrequencies)
  }

}

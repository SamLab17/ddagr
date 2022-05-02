package com.samlaberge

object WordApp extends DdagrApp {

  //  val ddagr = new Ddagr(DdagrOptions("localhost"))
  val ddagr = new Ddagr("frosted-mini-wheats.cs.utexas.edu")
  val start = System.nanoTime()
//  wordLength(ddagr)
    wordFrequencies(ddagr)
  val nSeconds = (System.nanoTime() - start) / 1e9
  println(s"${nSeconds} seconds elapsed")
  ddagr.exit()

  def wordLength(ddagr: Ddagr): Unit = {
    val res =
      ddagr.urlTextFiles(BookUrls.urls)
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
    val ds = ddagr.urlTextFiles(BookUrls.urls)
    val wordFrequencies = ds
      .flatMap(_.split("\\s+"))
      .map(_.trim)
      .map(_.toLowerCase)
      .filter(_.nonEmpty)
      .groupBy(identity)
      .mapGroups((word, words) => (word, words.size))
      .firstNBy(10, _._2, descending = true)
      .collect()
//    println(wordFrequencies)
  }

  def mapReduce[T, M, K, S](
    ds: Dataset[T],
    mapper: T => IterableOnce[M],
    keyFn: M => K,
    reducer: (K, Iterator[M]) => S
  ): Dataset[S] = {
    ds
      .flatMap(mapper)
      .groupBy(keyFn)
      .mapGroups(reducer)
  }

}

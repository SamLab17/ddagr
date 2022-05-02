package com.samlaberge

object WordApp extends DdagrApp {

  val ddagr = new Ddagr("frosted-mini-wheats.cs.utexas.edu")
  wordFrequencies(ddagr)
  ddagr.exit()


  def wordFrequencies(ddagr: Ddagr): Seq[(String, Int)] = {
    val ds = ddagr.urlTextFiles(BookUrls.urls)
    ds
      .flatMap(_.split("\\s+"))
      .map(_.trim)
      .map(_.toLowerCase)
      .filter(_.nonEmpty)
      .groupBy(identity)
      .mapGroups((word, words) => (word, words.size))
      .firstNBy(100, _._2, descending = true)
      .collect()
  }




















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

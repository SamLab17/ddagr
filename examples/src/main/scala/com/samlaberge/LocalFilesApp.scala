package com.samlaberge

/**
 * An example of syntax and use for ddagr
 */
object LocalFilesApp extends DdagrApp {

//  println(this.getClass.getName)
//  println(this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath)

  val ddagr = new Ddagr(DdagrOptions("localhost"))

//  case class Row(name: String, age: Int)
//  case class MappedRow(firstName: String, lastName: String, age: Int)
//
//  val r = Row("Sam Lab", 21)

//  val ds = ddagr.localTextFile("/Users/sam/example.txt")
//    .repartition(2)
//    .filter(_.length > 3)
//    .map(_ + "xx")
//  println(ds.collect())
//  println(ds.count())

  case class Person(
    first: String,
    last: String,
    age: Int,
    month: String,
    year: Int,
    city: String,
    stateOrProvince: String
  )

  val ds = ddagr.localTextFile("/Users/sam/example.csv")
    .filter(_.nonEmpty)
    .repartition(2)
    .map(s => {
      val fields = s.split(",")
      Person(
        fields(0),
        fields(1),
        fields(2).toInt,
        fields(3),
        fields(4).toInt,
        fields(5),
        fields(6)
      )
    })
    .filter(_.stateOrProvince == "Texas")
    .collect()
    println(ds)

//  val res = ddagr.mapTest(r, (r: Row) => {
//    MappedRow(
//      r.name.split("\\s+")(0),
//      r.name.split("\\s+")(1),
//      r.age
//    )
//  })
//  println(res)

//  val ds = ddagr.localTextFile("myFile.txt")
//  println(dagToString(
//    ds.map(_.toInt).filter(_ % 2 == 0))
//  )

//
//  val ds: DataSet[MyRowType] = ddagr.loadCsv("mydata.csv", csvLoadFn)
//  val mappedDs: Dataset[Int] = ds.map((x: MyRowType) => x.someIntField)
//  val filtered = mappedDs.filter((i: Int) => i % 2 == 0)
//  // This .count() is an action, actually sets off the task
//  val count: Int = filtered.count()
}

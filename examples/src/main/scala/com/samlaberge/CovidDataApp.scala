package com.samlaberge

import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Try

object CovidDataApp extends DdagrApp {
  val ddagr = new Ddagr("booberry.cs.utexas.edu")
  val ds = ddagr.localTextFile("covid.csv")
  case class Schema (
    date: Date,
    state: String,
    totalCases: Int,
    confirmedCases: Option[Int],
    probableCases: Option[Int],
    newCases: Int,
    probableNewCases: Option[Int],
    totalDeaths: Option[Int],
    confirmedDeaths: Option[Int],
    probableDeaths: Option[Int],
    newDeaths: Option[Int],
    probableNewDeaths: Option[Int],
    createdAt: Date,
    consentCases: Boolean,
    consentDeaths: Boolean
  )

  def parse(line: String): Option[Schema] = {
    val fields = line.split(",")
    def strToInt(s: String): Option[Int] = Try(s.toInt).toOption
    val dateFmt = new SimpleDateFormat("MM/dd/yyyy")
    val createdFmt = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa")
    Try {
      Schema(
        dateFmt.parse(fields(0)),
        state = fields(1),
        totalCases = fields(2).toInt,
        confirmedCases = strToInt(fields(3)),
        probableCases = strToInt(fields(4)),
        newCases = fields(5).toInt,
        probableNewCases = strToInt(fields(6)),
        totalDeaths = strToInt(fields(7)),
        confirmedDeaths = strToInt(fields(8)),
        probableDeaths = strToInt(fields(9)),
        newDeaths = strToInt(fields(10)),
        probableNewDeaths = strToInt(fields(11)),
        createdAt = createdFmt.parse(fields(12)),
        consentCases = fields(13) == "Agree",
        consentDeaths = fields(14) == "Agree"
      )
    }.toOption
  }

  val results = ds
    .map(parse)
    .filter(_.nonEmpty)
    .map(_.get)
    .groupBy(_.state)
    .mapValues(_.newCases)
    .reduceGroups(_ + _)
    .collect()

  println(results)
  ddagr.exit()
}

package com.demo.dataenggpoc

import org.apache.commons.lang3.ObjectUtils.median
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min}

object SparkAssignment3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Assignment3")
      .getOrCreate()

    //1. Load the Data and Create DataFrame
    val df = spark.read.option("header", "true")
      .csv("C:\\SparkExamples\\DataEnggPOC\\Data\\Capstone market analysis.csv")
    df.show()

    df.createTempView("Mycsv")

    val successDF = spark.sql("select count(*) as successCount from Mycsv where poutcome = 'success'")
    val failureDF = spark.sql("select count(*) as failureCount from Mycsv where poutcome = 'failure'")
    val numDF = spark.sql("select count(*) as totalCount from Mycsv")

    successDF.show()
    failureDF.show()
    numDF.show()




    val successCount = successDF.first().getLong(0)
    val failureRate = failureDF.first().getLong(0)
    println(successCount)
    val totalCount = numDF.first().getLong(0)
    println(totalCount)
    val MarketingSuccessRate = successCount.toDouble / totalCount
    //Give marketing failure rate
    val marketingFailureRate = failureRate.toDouble / totalCount

    println("Marketing Success Rate: " + MarketingSuccessRate)
    println("Marketing Failure Rate: " + marketingFailureRate)

    //3  Maximum, Mean, and Minimum age of average targeted customer
    val age = spark.sql("SELECT count(*)  FROM Mycsv")
    println("avg: " + df.select(avg("age")).collect()(0)(0))
    println("min: " + df.select(min("age")).collect()(0)(0))
    println("max: " + df.select(max("age")).collect()(0)(0))

    // 4. Check quality of customers by checking average balance , median balance of customers

    println("avgerage balance : " + df.select(avg("balance")).collect()(0)(0))

    val medianValue = df.select(median("balance")).collect()(0)(0)
    println(s"The median price is: $medianValue")

    // 5. Check if age matters in marketing subscription for deposit

    val agematters = spark.sql("select age, count(*) as number from  Mycsv where y='yes' group by age order by number desc")
      .show()

    //6.Check if marital status mattered for a subscription to deposit.
    val customers_by_marital = spark.sql("select marital, count(*) as number from Mycsv where y='yes' group by marital order by number desc")
      .show()

    //7 Check if age and marital status together mattered for a subscription to deposit scheme.
    val customers_by_agemarital = spark.sql("select age, marital, count(*) as number from  Mycsv where y='yes' group by age, marital order by number desc")
      .show()

    // 8. Do feature engineering for column—age and find right age effect on campaign
    val age_levels = spark.udf.register("age_levels", (age: Int) => {
      if (age <= 20)
        "Teen"
      else if (age > 20 && age <= 29)
        "Young_adult"
      else if (age > 29 && age <= 39)
        "Adult"
      else if (age > 39 && age < 49)
        "Older_adult"
      else if (age > 49 && age < 60)
        "Young_senior"
      else
        "Senior"
    })
    val effectoncampaign = df.withColumn("age", age_levels(df("age")))
    effectoncampaign.show()

    // 9 Plot the ‘age’ vs ‘y’ using any visualisation tool
  }

}

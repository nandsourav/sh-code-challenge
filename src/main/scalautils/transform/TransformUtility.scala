package scalautils.transform

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, length, _}
import org.testng.annotations.Test
import scalautils.sparkutils.CommonUtils

object TransformUtility {
  def main(args: Array[String]): Unit = {
    val mySparkSession = CommonUtils.returnSparkSession()
    val tableDf = CommonUtils.readFileIntoDataFrame(mySparkSession, "s3a://skyhook-hr/qa/sg_sample2/", "csv")
    def condition = col("_c6").gt(50.0)
      .or(length(split(col("_c5"), "\\.").getItem(1)).lt(5))
      .or(length(split(col("_c4"), "\\.").getItem(1)).lt(5))
      .or(col("_c1").isNull)
    val ignoreDf = tableDf.filter(condition)
    val filteredDf = tableDf.filter(!condition)
    val Row(latMinValue: Double, latMaxValue: Double, lonMinValue: Double, lonMaxValue: Double) = filteredDf.agg(min("_c4"), max("_c4"), min("_c5"), max("_c5")).head
    val tileUnitForLat = (latMaxValue - latMinValue)/100
    val tileUnitForLon = (lonMaxValue - lonMinValue)/100
    println("Number of rows that have invalid field values: "+ignoreDf.count())
    println("Total number of records processed: "+filteredDf.count())
    println("MBR parameters are as below")
    println("(0,0) maps to ("+lonMinValue+","+latMinValue+")")
    println("(99,99) maps to ("+lonMaxValue+","+latMaxValue+")")
    println("Length of 1 unit across X - axis is:"+tileUnitForLon)
    println("Length of 1 unit across Y - axis is:"+tileUnitForLat)

    def mapTilesX = floor((col("_c5")-lonMinValue)/tileUnitForLon)
    def mapTilesY = floor((col("_c4")-latMinValue)/tileUnitForLat)
    filteredDf.withColumn("MBR_X", when(mapTilesX === 100, 99).otherwise(mapTilesX))
              .withColumn("MBR_Y", when(mapTilesY === 100, 99).otherwise(mapTilesY))
              .withColumn("MBR_XY", concat(lit("("), col("MBR_X"), lit(","), col("MBR_Y"), lit(")")))
              .withColumn("count", count("*").over(Window.partitionBy("_c1", "MBR_X", "MBR_Y")))
              .withColumn("rank", dense_rank().over(Window.partitionBy("_c1").orderBy(col("count").desc)))
              .select("_c1", "MBR_XY", "rank").distinct().where(col("rank").isin(1,2,3)).orderBy("_c1")
              .groupBy("_c1","rank").pivot("rank").agg(collect_list("MBR_XY"))
              .groupBy(col("_c1").as("device_id")).agg(
                     trim(collect_list("1")(0).cast("string"),"[\\[\\]]").as("first_most_common")
                    ,trim(collect_list("2")(1).cast("string"),"[\\[\\]]").as("second_most_common")
                    ,trim(collect_list("3")(2).cast("string"),"[\\[\\]]").as("third_most_common")
              )
              .write.mode("overwrite").parquet("output")
  }
}

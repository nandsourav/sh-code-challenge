package scalautils.sparkutils

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.sql.{DataFrame, SparkSession}


object CommonUtils {
  def returnSparkSession(): SparkSession ={
    var sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("GI Coding Challenge")
      .getOrCreate()

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", new DefaultAWSCredentialsProviderChain().getCredentials.getAWSAccessKeyId)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", new DefaultAWSCredentialsProviderChain().getCredentials.getAWSSecretKey)
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }

  /**
    * Read in files from a location into a dataframe and return the dataframe
    * @param tableLocation
    * @param fileType
    * @return
    */
  def readFileIntoDataFrame(sparkS: SparkSession, tableLocation: String, fileType: String): DataFrame = {

    try {
      val tableDf: DataFrame = fileType match {
        case "parquet" => sparkS.read.parquet(tableLocation)
        case "csv" => sparkS.read.option("inferSchema", "true").option("multiLine", "true").csv(tableLocation)
        case "json" => sparkS.read.json(tableLocation)
        case "avro" => sparkS.read.format("com.databricks.spark.avro").load(tableLocation)
        case _ => throw new RuntimeException("File Type \"" + fileType + "\" was not found in match statement")
      }

      tableDf
    }
    catch {
      case ex: Exception => {
        System.err.println("ERROR: Failure in readFileIntoDataFrame - fileType = " + fileType + " |  tableLocation = " + tableLocation)
        System.out.println("ERROR: Failure in readFileIntoDataFrame - fileType = " + fileType + " |  tableLocation = " + tableLocation)
        throw ex
      }
    }
  }
}

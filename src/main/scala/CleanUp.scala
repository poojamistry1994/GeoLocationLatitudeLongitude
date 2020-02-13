import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object CleanUp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CleanUp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sq = new org.apache.spark.sql.SQLContext(sc)
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)

   val POI_df = hc
      .read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load("C:\\Users\\PoojaMistry\\Documents\\ws-data-spark-master\\data/POIList.csv")

    val datadf= hc
      .read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load("C:\\Users\\PoojaMistry\\Documents\\ws-data-spark-master\\data/DataSample.csv")


    val datatemp = datadf.withColumn("cnt",row_number().over(Window.partitionBy("` TimeSt`","Latitude","Longitude")))
        .selectExpr("_ID","` TimeSt` as TimeSt","Latitude","Longitude","cnt")

    datatemp
      .filter(datatemp("cnt")===1).drop("cnt")
      .repartition(1)
      .write.mode("overwrite").option("header","true").format("com.databricks.spark.csv")
      .save("C:\\Users\\PoojaMistry\\Documents\\ws-data-spark-master\\test/FinalData.csv")

    val poitemp = POI_df.withColumn("cnt",row_number().over(Window.partitionBy("` Latitude`","Longitude")))
        .selectExpr("POIID","` Latitude` as Latitude","Longitude","cnt")

    poitemp.filter(poitemp("cnt")===1).drop("cnt")
      .repartition(1)
      .write.format("com.databricks.spark.csv").option("header","true").mode("overwrite")
      .save("C:\\Users\\PoojaMistry\\Documents\\ws-data-spark-master\\test/FinalPOI.csv")


  }
}
package com.GeographicLoc.formatter

import com.GeographicLoc.formatter.format.Coordinate
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}

object Search{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Label")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Setting log level to error
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    // file path
    val masterDataPath = "C:\\Users\\PoojaMistry\\Documents\\ws-data-spark-master\\test\\FinalPOI.csv"
    val searchDataPath = "C:\\Users\\PoojaMistry\\Documents\\ws-data-spark-master\\test\\FinalData.csv"
    val outputPath = "C:\\Users\\PoojaMistry\\Documents\\ws-data-spark-master\\test\\output\\geolocation"

    // Use of scala curring and partial functions
    val getCsvPartialDF = getCsv(sqlContext)_

    // Reading DataFrames for OriginalDataSet and ContinuousDataSet
    val masterDataFrame = getCsvPartialDF(masterDataPath)
    val searchDataFrame = getCsvPartialDF(searchDataPath)

    // Limits
    val latLimit = 0.5
    val lonLimit = 4.0
    val maxDist = 500.0

    // Broadcasting Master Data
    val broadcastMasterDF = broadcastMasterDataFrame(sqlContext, masterDataFrame)

    val solDF = search(sqlContext, broadcastMasterDF, searchDataFrame, latLimit, lonLimit, maxDist)


    solDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save(outputPath)
  }


  def search(sqlContext: SQLContext, broadcastIndex: Broadcast[List[Coordinate]], searchDataFrame: DataFrame, latLimit: Double, lonLimit: Double, maxDist: Double): DataFrame = {
    val indexList = broadcastIndex.value


    val mapper = (searchId: String, searchLat: Double, searchLon: Double) => {

      // Filtering out the index coordinates
      // TODO: This Filter logic can be implemented using binary search to reduce complexity.
      val filterIndexList = indexList.filter{indexCoordinate =>
        val indexLat = indexCoordinate.lat
        val indexLon = indexCoordinate.lon
        // Filter Predicate
        (searchLat - latLimit < indexLat || searchLat + latLimit > indexLat) &&
          (searchLon - lonLimit < indexLon || searchLon + lonLimit < indexLon)
      }

      // Calculate distance for all indices left and take the min one
      filterIndexList match {
        case a if filterIndexList.nonEmpty =>
          val search = Coordinate(searchId, searchLat, searchLon)
          val nearestCoordinateDistTuple = filterIndexList
            .map(c => (c, geoDist(c, search)))
            .reduce((a, b) => if (a._2 < b._2) a else b)
          if (nearestCoordinateDistTuple._2 < maxDist)
            Some(nearestCoordinateDistTuple._1)
          else
            None
        case _ => // No match found
          None
      }
    }

    val mapper_udf = udf(mapper)

    searchDataFrame.withColumn("_mappedCoordinate", mapper_udf(col("_ID"), col("Latitude"), col("Longitude")))
      .select(col("_ID"), col("_mappedCoordinate.id").as("POIID"))
  }



  def broadcastMasterDataFrame(sqlContext: SQLContext, df: DataFrame): Broadcast[scala.List[Coordinate]] = {
    val castDF = df
      .withColumn("Latitude", col("Latitude").cast(DoubleType))
      .withColumn("Longitude", col("Longitude").cast(DoubleType))
      .coalesce(1) // distributed sorting is not used since the index would be fairly small in size
      .sort(asc("Latitude"), asc("Longitude"))
      .map(toCoordinate)

    sqlContext.sparkContext.broadcast(castDF.collect().toList)
  }


  def geoDist(ilat1: Double, ilon1: Double, ilat2: Double, ilon2: Double): Double = {
    val long2 = ilon2 * math.Pi / 180
    val lat2 = ilat2 * math.Pi / 180
    val long1 = ilon1 * math.Pi / 180
    val lat1 = ilat1 * math.Pi / 180

    val dlon = long2 - long1
    val dlat = lat2 - lat1
    val a = math.pow(math.sin(dlat / 2), 2) + math.cos(lat1) * math.cos(lat2) * math.pow(math.sin(dlon / 2), 2)
    val c = 2 * math.atan2(Math.sqrt(a), math.sqrt(1 - a))
    val result = 6373 * c

    result
  }


  def geoDist(c1: Coordinate, c2: Coordinate): Double = {
    geoDist(c1.lat, c1.lon, c2.lat, c2.lon)
  }


  def getCsv(sqlContext: SQLContext)(path: String): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(path)
  }


  def toCoordinate(row: Row): Coordinate = Coordinate(row.getString(0), row.getDouble(1), row.getDouble(2))

}

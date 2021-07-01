package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.expr
import scala.math.{pow, sqrt}


object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep  // Jiayin Wang - in the instruction, the box min is 74.25
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  // Calculate X_bar = average number of pick-up points in one cell
  val x_mean:Double = pickupInfo.count().toDouble / numCells.toDouble

  // Calculate attribute value x for each cell. x_i = count of points within this cell i
  var pickupCell = pickupInfo.groupBy("x", "y", "z").count().withColumnRenamed("count", "attr")  // rename x_i to attr_i to differentiate from coordinate
  
  // add x_i squred to each cell
  pickupCell = pickupCell.withColumn("attr_2", expr("attr*attr"))
  pickupCell.show()

  // calculate S
  var S:Double = pickupCell.agg(sum("attr_2")).first.getLong(0).toDouble
  S = sqrt(S / numCells.toDouble - pow(x_mean, 2))
  println("S="+S)

  // add two more columns to record neighboring cell's attribute value
  pickupCell = pickupCell.drop("attr_2") // no need this attr^2 col
  var neighborCellAttr = pickupCell.as("df1").join(pickupCell.as("df2"),
    (col("df1.x") === col("df2.x") - 1 || col("df1.x") === col("df2.x") + 1 || col("df1.x") === col("df2.x")) &&
    (col("df1.y") === col("df2.y") - 1 || col("df1.y") === col("df2.y") + 1 || col("df1.y") === col("df2.y")) &&
    (col("df1.z") === col("df2.z") - 1 || col("df1.z") === col("df2.z") + 1 || col("df1.z") === col("df2.z"))// &&
    //!(col("df1.x") === col("df2.x") && col("df1.y") === col("df2.y") && col("df1.z") === col("df2.z")),
    ,"inner").select(col("df1.x").as("x"), col("df1.y").as("y"), col("df1.z").as("z"), col("df2.attr").as("attr"))
  println("displaying neighboring cell attribute value")
  neighborCellAttr.show()

  // calculate sigma_j=1_N(x_j), and N
  val neighborCountAgg = neighborCellAttr.groupBy("x", "y", "z").agg(count("attr").as("N"), sum("attr").as("sum_attr"))
  println("displaying aggregated neighboring cell count and attribute value")
  neighborCountAgg.show()

  // calculate G
  spark.udf.register("calculateG", (attr_agg:Int, N:Int) => ((HotcellUtils.calculateG(attr_agg.toDouble, N.toDouble, numCells.toDouble, x_mean, S))))
  neighborCountAgg.createOrReplaceTempView("neighborCountAgg")
  val cellGScore = spark.sql("select x, y, z, calculateG(sum_attr, N) as G from neighborCountAgg order by G DESC, x DESC, y DESC, z DESC")
  println("displaying G score")
  cellGScore.show()

  val ret = cellGScore.select(col("x"), col("y"), col("z")) // YOU NEED TO CHANGE THIS PART
  println("displaying ret")
  ret.show()
  return ret.coalesce(1)
}
}

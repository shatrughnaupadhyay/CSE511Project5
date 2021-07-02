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
  pickupInfo.createOrReplaceTempView("allPickUps")
  spark.udf.register("CheckContain",(x:Int, y:Int, z:Int)=>(HotcellUtils.CheckContain(x,y,z)))
  val pointsInCube = spark.sql("select x,y,z from allPickUps where CheckContain(x, y, z)")
  pointsInCube.createOrReplaceTempView("pointsInCube")

  val points = spark.sql("select count(*) as totalPoint from pointsInCube")
  points.createOrReplaceTempView("points")
  val pointsNumber = points.first().getLong(0)


  val countCube = spark.sql("select count(*) as totalPoints, power(count(*), 2) as squarePoints ,  x, y, z from pointsInCube group by x,y,z")
  countCube.createOrReplaceTempView("countCube")
  val cubesNumber = countCube.count()

  println(pointsNumber)
  println(cubesNumber)
  val avgX = pointsNumber.toDouble / cubesNumber.toDouble

  val squarePointsDf = spark.sql("select sum(squarePoints) from countCube")
  val squarePoints = squarePointsDf.first().getDouble(0)
  val largeS = sqrt((squarePoints / cubesNumber) - (pow(avgX, 2)))
  println(largeS)
  return pointsInCube
}
}

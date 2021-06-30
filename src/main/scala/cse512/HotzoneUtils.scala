package cse512
import scala.math.{max, min}

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    var rc = queryRectangle.split(',')    // do not use .map(_.toDouble) here because of the sbt version issue
    var upperLeftX = min(rc(0).toDouble, rc(2).toDouble)
    var upperLeftY = max(rc(1).toDouble, rc(3).toDouble)
    var bottomRightX = max(rc(0).toDouble, rc(2).toDouble)
    var bottomRightY = min(rc(1).toDouble, rc(3).toDouble)

    var pc = pointString.split(',')

    return pc(0).toDouble >= upperLeftX & pc(0).toDouble <= bottomRightX & pc(1).toDouble <= upperLeftY & pc(1).toDouble >= bottomRightY
  }

  // YOU NEED TO CHANGE THIS PART

}

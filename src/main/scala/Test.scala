import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  val url = "jdbc:mysql://localhost/ip_forcrawl?useUnicode=true&characterEncoding=utf-8&useSSL=false"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    var DRDD = sc.textFile(Test.getClass.getResource("/") + "main.csv")
    import sqlContext.implicits._
    var temp = DRDD.map(line => {
      var t = line.split(",")
      (t(1), t(2), t(3), t(4), t(5), t(6), t(10))
    }).toDF("typeId", "mediaId", "tagId", "name", "areaId", "year", "isVip")
    temp.show()
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root")
    updatePlay_main("fname")
  }

  def updatePlay_main(tableName: String): Unit = {
    val connection = DriverManager.getConnection(url, "root", "root")
    val statement = connection.createStatement()
    statement.execute("UPDATE play_main SET startTime=CURRENT_TIME,vipEndTime=CURRENT_TIME WHERE play_main.name IN (SELECT name FROM " + tableName + ")")
    statement.close()
    connection.close()
  }
}
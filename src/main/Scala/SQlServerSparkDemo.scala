import java.util.Properties

import org.apache.spark.sql.SparkSession
import java.sql.DriverManager;
import java.sql.Connection
import java.util.Properties
import org.apache.log4j.{Logger,Level}


object SQlServerSparkDemo {

  def main(args: Array[String]): Unit = {

    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession.builder().
      appName("SQLServerSparkDemo").
      config("spark.master", "local").
      enableHiveSupport().
      getOrCreate()

    // Create a Dataframe from MSSql Server course catalog table.

    // set variable to be used to conn
    val database = "AdventureWorksDW2017"
    val table = "AdventureWorksDW2017.dbo.DimCustomer"
    val user = "AdvETL"
    val password  = "interface1987"
    val server_name = """DESKTOP-02A5G71\SQLEXPRESS"""
    val instanceName= "SQLEXPRESS"

    // read table data into a spark dataframe
    val jdbcDF = spark.read.format("jdbc")
    .option("url", s"jdbc:sqlserver://192.168.1.5:1433;instanceName=${instanceName};databaseName=${database}")
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()

    //show the data loaded into dataframe
      //jdbcDF.show()
    //jdbcDF.select("CustomerKey").show(false)
    //jdbcDF.write.csv("file:///C:/Documents/jdbcDF1.csv")
    jdbcDF.write.mode("overwrite").saveAsTable("jdbcDFSample")

    spark.sql("select * from jdbcDFSample").show(false)


  }

}

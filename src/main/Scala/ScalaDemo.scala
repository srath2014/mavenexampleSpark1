import java.util.Properties

import org.apache.spark.sql.SparkSession

object ScalaDemo {

  def main(args: Array[String]): Unit = {

    println("Helloworld")

    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    //.config("spark.sql.warehouse.dir",C:\Users\Miku\IdeaProjects\mavenexampleSpark).enableHiveSupport()

    val spark = SparkSession.builder().
      appName("SparkScalaDemo").
      config("spark.master", "local").
      enableHiveSupport().
      getOrCreate()

    println("Created Spark session")

    val simpleSeq = Seq((1,"Spark"),(2,"Haddop"))

    val df = spark.createDataFrame(simpleSeq).toDF("courseId","CourseName")

    //df.write.format("csv").save("sampleseq")

    // Create a Dataframe from Postgres course catalog table.

    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","interface1987")

    val pgTable = "futureschema.futurex_course_catalog"
    //server:port/database_name

    val pgCourseDataframe = spark.read.jdbc("jdbc:postgresql://localhost:5432/futurex",pgTable,pgConnectionProperties)

    println("Fetched")

    pgCourseDataframe.show()
    println("Shown")

  }
}

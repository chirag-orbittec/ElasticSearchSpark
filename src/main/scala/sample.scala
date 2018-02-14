import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.{LabeledPoint, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types.DateType



object sample {

  def main(args: Array[String]): Unit = {




    println("hello world")
    val query = "\"match_all\" : {} "


    val conf = new SparkConf().setMaster("local[2]")
      //.set("es.query",query)
      //.set("es.nodes", "localhost")
      //.set("es.port", "9200")
     
      //.set("es.endpoint", "_search")

    val spark = SparkSession
      .builder()
      .appName("SampleApp").config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    conf.set("es.index.auto.create", "true")

    // define a case class
    //case class Trip(departure: String, arrival: String)

    //val upcomingTrip = Trip("OTP", "SFO")
    //val lastWeekTrip = Trip("MUC", "OTP")

    //val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    //EsSpark.saveToEs(rdd, "spark/docs")








//
//
//    val temp = EsSpark.
//    val newRdd = rdd.foreach(element => element._2)
//

    val df =   spark.read.format("org.elasticsearch.spark.sql").load("**********")

    df.printSchema()

    df.createOrReplaceTempView("WAFER_COUNTS")

    val sqlDF = spark.sql("SELECT toolId as TOOL_ID,lotEndDate as LOT_END_DATE,waferCount as WAFER_COUNT FROM WAFER_COUNTS order by LOT_END_DATE")

    sqlDF.show()
     val first  = sqlDF.first().get(1)
    //val newDF = sqlDF.groupBy(dayofmonth(sqlDF.col("LOT_END_DATE")).alias("LOT_START_DATE")).agg(sum("WAFER_COUNT"))
    //newDF.show()
    var newDF = sqlDF.groupBy(window(sqlDF.col("LOT_END_DATE"),"1 day"),col("TOOL_ID")).agg(sum("WAFER_COUNT").alias("C")).orderBy("window")
      //.withColumn("rownum",monotonically_increasing_id()+1)

    newDF.printSchema()

    //var explodeDF = newDF.withColumn("window",explode(col("window")) )

    newDF.createOrReplaceTempView("WAFER_COUNTS")

    newDF = spark.sql("SELECT C as label,window.start as startTime FROM WAFER_COUNTS ")


    newDF = newDF.withColumn("newTime",unix_timestamp(col("startTime")))
    newDF.show()

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val date =  format.parse(first.toString)
    val unixTime = date.getTime()
    val myFunc = udf{(x:String) =>
      val xdate =  format.parse(x)
      val xunixTime = xdate.getTime()
      //((xunixTime-unixTime):Double)/1000000
      xunixTime:Double
    }




    newDF = newDF.withColumn("newTime",myFunc(col("startTime")))
    newDF.show()

   import df.sqlContext.implicits._
    val labeled = newDF.map(
      row => {
        LabeledPoint((row.getAs[Long]("label")).toDouble, Vectors.dense(row.getAs[Double]("newTime")))
      }   ).toDF()

    val lr = new LinearRegression()
      .setMaxIter(10).setRegParam(100.0)




    val model = lr.fit(labeled)
    val prediction = model.transform(labeled)


    prediction.show(300)
  }
}

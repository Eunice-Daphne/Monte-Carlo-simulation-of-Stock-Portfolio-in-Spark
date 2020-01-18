import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, RowFactory}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable
/**
  *Monte Carlo Simulations
  */
class Simulation {

  val numTrails = Main.numTrails
  val sqlContext = Main.sqlContext
  val logger = Main.logger
  /**
   * Monte Carlo Simulation to record the percentage loss and gains for each ticker separately
   * @param dateAndTickerToChangeRDD : RDD which stores the (Date,Ticker) and corresponding Percentage Change in stock Close value
   * @param numberofStocks : Number of stocks invested in
   * @param tickerAndWeights : A Map of Ticker and their weights
   * @param tickerList : List containing all the tickers
   * @return : percentage loss/gain of each ticker
   */
  def individualTickers(dateAndTickerToChangeRDD : RDD[(String, Float)], numberofStocks : Int, tickerAndWeights : collection.Map[String, Float], tickerList: Seq[String]): mutable.Map[String, (Float, Float, Float)] ={
    // Group the RDD on Date and Ticker
    val groupedDateAndTickerToChangeRDD = dateAndTickerToChangeRDD.groupByKey()
    val numEvents = groupedDateAndTickerToChangeRDD.count

    logger.debug("Grouped DateAndTicker To Change RDD\n", groupedDateAndTickerToChangeRDD.take(10))
    //Begin the trails by sampling the RDD and record the result
    val fraction = 1.0 * numTrails / numEvents
    val resultOfTrials = groupedDateAndTickerToChangeRDD.sample(true, fraction).map(
      i => {
        var total = 0f
        for (t <- i._2) {
          val symbol = i._1.toString.split(",").toList(1)
          val changeInPrice = t.toString.toFloat
          val weight = tickerAndWeights.get(symbol).get
          total += changeInPrice * weight
        }
        new Tuple2[String, Float](i._1, total)
      }
    )
    logger.info("End of Initial Simulation")
    //Create an RDD row from the result of the trails
    val resultOfTrialsRows: RDD[Row] = resultOfTrials.map(x => RowFactory.create(x._1.toString.split(",").toList(0) , x._1.toString.split(",").toList(1), math.ceil(x._2.toDouble*100).asInstanceOf[Object]))
    // Define a schema to load to a DataFrame
    val schema : StructType = StructType(
      List(
        StructField("date", DataTypes.StringType, false),
        StructField("ticker", DataTypes.StringType, false),
        StructField("changePct", DataTypes.DoubleType, false)
      )
    )
    // Create a Dataframe from the result of trails
    val resultOfTrialsDF : DataFrame = sqlContext.createDataFrame(resultOfTrialsRows, schema)
    resultOfTrialsDF.createOrReplaceTempView("TrailsDF")
    //Record the percentage of loss/gain of each ticker from the DataFrame
    var ticker_probability = scala.collection.mutable.Map[String, (Float, Float, Float)]()
    for (ticker <- tickerList) {
      val percentilesRow = sqlContext.sql("select percentile(changePct, array(0.05,0.50,0.95)) from TrailsDF where ticker='"+ ticker +"'").collectAsList().get(0).getList(0)
      ticker_probability += (ticker -> (percentilesRow.get(0).toString().toFloat/100, percentilesRow.get(1).toString().toFloat/100, percentilesRow.get(2).toString().toFloat/100))
    }
    ticker_probability
  }

  /**
   * Monte Carlo Simulation to record the percentage loss and gains for all tickers combined
   * @param dateToTickerAndChangeRDD : RDD which stores the Date and corresponding (Ticker,Percentage Change in stock Close value)
   * @param numberofStocks : Number of stocks invested in
   * @param all_tickerSymbolAndWeight : A Map of Ticker and their weights
   * @param totalInvestement : Total Amount Invested
   * @return
   */
  def allTickersCombined(dateToTickerAndChangeRDD: RDD[(String, (String, Float))], numberofStocks: Int, all_tickerSymbolAndWeight: collection.Map[String, Float], totalInvestement: Long): util.List[Nothing] = {
    // Group the RDD on Date
    val groupedDatesToTickerAndChangeRDD = dateToTickerAndChangeRDD.groupByKey()
    //Filter out dates that do not have all the stock values
    val countsByDate: collection.Map[String, Long] = dateToTickerAndChangeRDD.countByKey
    val filterdDatesToTickerAndChangeRDD = groupedDatesToTickerAndChangeRDD.filter(x => countsByDate.get(x._1).get >= numberofStocks)
    val numEvents = filterdDatesToTickerAndChangeRDD.count
    logger.debug("Symbols and Weights\n"+all_tickerSymbolAndWeight)
    //println("Symbols and Weights")
    //Begin the trails by sampling the RDD and record the result
    val fraction = 1.0 * numTrails / numEvents
    val resultOfTrials: RDD[(String, Float)] = filterdDatesToTickerAndChangeRDD.sample(true, fraction).map(
      i => {
        var total = 0f
        for (t <- i._2) {
          val symbol = t._1.toString
          val changeInPrice = t._2.toString.toFloat
          val weight = all_tickerSymbolAndWeight.get(symbol).get
          total += changeInPrice * weight
        }
        new Tuple2[String, Float](i._1, total)
      }
    )
    logger.info("End of Second Monte Carlo Simulation")
    logger.info("total runs: " + resultOfTrials.count())
    //Create an RDD row from the result of the trails
    val resultOfTrialsRows: RDD[Row] = resultOfTrials.map(x => RowFactory.create(x._1 , math.ceil(x._2*100).asInstanceOf[Object]))
    // Define a schema to load to a DataFrame
    val schema : StructType = StructType(
      List(
        StructField("date", DataTypes.StringType, false),
        StructField("changePct", DataTypes.DoubleType, false)
      )
    )
    // Create a Dataframe from the result of trails
    val resultOfTrialsDF : DataFrame = sqlContext.createDataFrame(resultOfTrialsRows, schema)
    resultOfTrialsDF.createOrReplaceTempView("results")
    //Record the percentage of loss/gain of all ticker in a single day
    val percentilesRow: util.List[Nothing] = sqlContext.sql("select percentile(changePct, array(0.05,0.50,0.95)) from results").collectAsList().get(0).getList(0)

    percentilesRow
  }


}

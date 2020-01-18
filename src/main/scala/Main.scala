import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/**
 * Spark program for parallel processing of the predictive engine for stock portfolio losses using Monte Carlo Simulation
 */
object Main {

  val logger = LoggerFactory.getLogger(this.getClass)
  val local_conf = ConfigFactory.load("Input")
  val spark = SparkSession.builder().appName("MonteCarlo").getOrCreate
  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext
  val numTrails = 1000000

  val obj = new Simulation()

  def main(args: Array[String]): Unit = {

    val stockDataDir = args(0)
    /**
     * Read the total amount invested, number of stocks to invest with their symbols from the Config file
     */
    val totalInvestement = local_conf.getString("TotalAmount").toLong
    val numberofStocks = local_conf.getString("Number_of_stocks").toInt
    val tickerList = local_conf.getString("Ticker_list").split(",").toList

    /**
     * Initially, amount is equally distributed among all the stocks
     */
    val distribution = 1/numberofStocks.toFloat
    val tickerRDD: RDD[String] = createRDD(tickerList)
    val tickerAndDollarsRDD: RDD[(String, Float)] = tickerRDD.map(x => (x, totalInvestement*distribution))
    /**
     * Obtain the weight of individual stock as part of the total money invested. At this point, the weights of all the stocks are equal
     */
     val tickerAndWeights: collection.Map[String, Float] = calculateTickerWeight(tickerAndDollarsRDD, totalInvestement)

    logger.debug("Total Investment : \n"+ totalInvestement)
    logger.debug("Initial Weights of Ticker :\n" + tickerAndWeights)

    /**
     * Form an RDD of the key value form ((Date,Ticker Symbol), Percentage Change in Stock Close Value) from the Stock Data .csv files for all the Tickers
     * @param pairDateSym : (Date,Ticker Symbol)
     * @param change : Percentage Change in Stock Close Value
     */
    case class keyValuePairOne(pairDateSym: String, change: Float)
    val dateAndTickerToChangeRDD: RDD[(String, Float)] = sc.textFile(stockDataDir).filter(x => !x.startsWith("Date")).map(
      x => {
          val splits = x.split(",", -2)
          // date->splits(0), symbol->splits(7), changeInPrice->splits(8) from the stockData csv files
          keyValuePairOne(splits(0)+","+splits(7), splits(8).toFloat)
      }
    ).map(x => (x.pairDateSym, x.change))
    logger.debug("DateAndTicker To Change  RDD \n"+dateAndTickerToChangeRDD.take(5))
    /**
     * Start a Monte Carlo Simulation to record the percentage loss and gains for each ticker
     */
    val ticker_probability =  obj.individualTickers(dateAndTickerToChangeRDD, numberofStocks, tickerAndWeights, tickerList)
    logger.debug("Ticker porbablity: \n"+ticker_probability)
    /**
     * Obtain lists of tickers that gained and lost separately
     */
    val loss_ticker = listOfLossTickers(ticker_probability)
    val gain_ticker = listOfGainTickers(ticker_probability)
    val loss_count = loss_ticker.length
    logger.debug("Loss Count: "+loss_count)
    logger.debug("Loss ticker: "+loss_ticker)
    logger.debug("Gain ticker: "+ gain_ticker)
    /**
     * Obtain a new stock invest amount distribution based on the most likely scenario;
     * 1. Reduce the amount invested on stocks that recorded a loss or plateaued by half by selling them
     * 2. Distribute the amount obtained from selling the stocks to those stocks that recorded profits
     */
    val new_distribution_loss = (distribution*loss_count) / 2
    val new_distribution_gain = 1 - new_distribution_loss
    logger.debug("new_distribution_loss: "+new_distribution_loss)
    logger.debug("new_distribution_gain: "+ new_distribution_gain)
    val loss_tickerRDD: RDD[String] = createRDD(loss_ticker.toList)
    val loss_tickersymbolsAndDollarsRDD = loss_tickerRDD.map(x => (x, (new_distribution_loss/loss_count)*totalInvestement))
    val gain_tickerRDD: RDD[String] = createRDD(gain_ticker.toList)
    val gain_tickersymbolsAndDollarsRDD: RDD[(String, Float)] = gain_tickerRDD.map(x => (x, (new_distribution_gain/(numberofStocks-loss_count))*totalInvestement))
    /**
     * Combine the Gain and Loss tickers as one RDD and assign weight for each stock as part of the total money invested
     */
    val allTickersAndDollar: RDD[(String, Float)] = loss_tickersymbolsAndDollarsRDD ++ gain_tickersymbolsAndDollarsRDD
    logger.debug("Symbols and Dollars:\n"+allTickersAndDollar)
     val all_tickerSymbolAndWeight: collection.Map[String, Float] = calculateTickerWeight(allTickersAndDollar,totalInvestement)

    logger.debug("Symbols and Weights:\n"+all_tickerSymbolAndWeight)
    /**
     * Form an RDD of the key value form (Date, (Ticker Symbol,Percentage Change in Stock Close Value)) from the Stock Data .csv files for all the Tickers
     * @param date : Date of the recorded stock
     * @param pairSymChg : (Ticker Symbol,Percentage Change in Stock Close Value)
     */
    case class keyValuePairTwo(date: String, pairSymChg: Tuple2[String, Float])
    val dateToTickerAndChangeRDD: RDD[(String, (String, Float))] = sc.textFile(stockDataDir).filter(x => !x.startsWith("Date")).map(
      x => {
        val splits = x.split(",", -2)
        // date->splits(0), symbol->splits(7), changeInPrice->splits(8) from the stockData csv files
        keyValuePairTwo(splits(0), (splits(7), splits(8).toFloat))
      }
    ).map(x => (x.date, x.pairSymChg))
    /**
     * Start a second Monte Carlo Simulation to record the percentage loss and gains for all the stocks combined
     */
     val percentilesRow = obj.allTickersCombined(dateToTickerAndChangeRDD, numberofStocks, all_tickerSymbolAndWeight, totalInvestement)
    /**
     * Record the performance of the stock in a single day by Worst Case, Most likely and Best Case
     */
     val worstCase = percentilesRow.get(0).toString.toFloat/100
     val mostLikely = percentilesRow.get(1).toString.toFloat/100
     val bestCase = percentilesRow.get(2).toString.toFloat/100

     val initial = "Your initial investment is $"+totalInvestement+"\n"
     val worst = "In the Worst Case your stock holding could be $"+ (Math.round(totalInvestement * worstCase / 100).toFloat+totalInvestement)
     val most = "In the Most Likely Case your stock holding could be $"+ (Math.round(totalInvestement * mostLikely / 100).toFloat+totalInvestement)
     val best = "In the Best Case your stock holding could be $"+ (Math.round(totalInvestement * bestCase / 100).toFloat+totalInvestement)

     logger.debug(initial+"\n"+worst+"\n"+most+"\n"+best)
      println(initial+"\n"+worst+"\n"+most+"\n"+best)

     val output : RDD[String]= sc.parallelize(List(initial,worst,most,best))

     //Output Directory
     output.saveAsTextFile(args(1))

     sc.stop
  }

  //Create RDD for a list of tickers
  def createRDD(ticker : List[String]): RDD[String]  = {
    val tickerRDD: RDD[String] = sc.parallelize(ticker)
    tickerRDD
  }

  //Create a map of Ticker with corresponding weights
  def calculateTickerWeight(allTickersAndDollar : RDD[(String, Float)], totalInvestement: Long): collection.Map[String, Float] ={
    val all_tickerSymbolAndWeight = allTickersAndDollar.map(x => new Tuple2[String, Float](x._1, x._2/totalInvestement)).collectAsMap()
    all_tickerSymbolAndWeight
  }

  //Create a list of Loss tickers
  def listOfLossTickers(ticker_probability : mutable.Map[String, (Float, Float, Float)] ): ListBuffer[String] ={
    var loss_ticker = ListBuffer[String]()
    for (estimate <- ticker_probability)
      if (estimate._2._2 <= 0) loss_ticker += estimate._1.toString
    loss_ticker
  }

  //Create a list of Gain Tickers
  def listOfGainTickers(ticker_probability : mutable.Map[String, (Float, Float, Float)] ): ListBuffer[String] ={
    var gain_ticker = ListBuffer[String]()
    for (estimate <- ticker_probability)
      if (estimate._2._2 > 0) gain_ticker += estimate._1.toString
    gain_ticker
  }

}

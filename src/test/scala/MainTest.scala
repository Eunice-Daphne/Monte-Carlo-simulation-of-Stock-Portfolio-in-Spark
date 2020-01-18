import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable


class MainTest extends FlatSpec{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val local_conf = ConfigFactory.load("Input")
  val spark = SparkSession.builder().master("local[*]").appName("MonteCarlo Test").getOrCreate


  "read config" should "return values from config file" in {

    val total_amount = local_conf.getString("TotalAmount")
    val No_of_stocks = local_conf.getString("Number_of_stocks")
    val Ticker_list = local_conf.getString("Ticker_list")

    assert(total_amount != null)
    assert(No_of_stocks != null)
    assert(Ticker_list != null)

  }

  "Verify createRDD" should "return RDD of String when list is passed" in {

    val tickers = local_conf.getString("Ticker_list").split(",").toList
    val tickerRDD = Main.createRDD(tickers)

    assert(tickerRDD != null)
    assert(tickerRDD.count() == tickers.length)

  }

  "Verify listOfLossTickers" should "return list of Tickers whose value is plateaued/depreciated" in {

    val input_Ticker = mutable.Map("AAPL"-> (-0.3F,0.2F,0.4F), "OIL" -> (-0.4F,-0.1F,0.3F))
    val lossTickerList = Main.listOfLossTickers(input_Ticker)

    assert(lossTickerList != null)
    assert(lossTickerList.length == 1)
    assert(lossTickerList(0) == "OIL")

  }

  "Verify listOfGainTickers" should "return list of Tickers whose value has profited" in {

    val input_Ticker = mutable.Map("GE"-> (-0.35F,0.2F,0.67F), "GS" -> (-0.4F,-0.3F,0.58F))
    val gainTickerList = Main.listOfGainTickers(input_Ticker)

    assert(gainTickerList != null)
    assert(gainTickerList.length == 1)
    assert(gainTickerList(0) == "GE")

  }

  "Verify calculateTickerWeight" should "return weight for each ticker" in {

    val sc = spark.sparkContext
    val input = sc.parallelize(List("MSI","AAPL")).map(x => (x, 500F))
    val input_Ticker = Main.calculateTickerWeight(input,1000L)
    //println(input_Ticker.foreach(println))
    assert(input_Ticker.get("MSI").get == 0.5F)
    assert(input_Ticker.get("AAPL").get == 0.5F)
    sc.stop()

  }





}

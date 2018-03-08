package edu.knoldus

import java.sql.DriverManager

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status


object twitterApp extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("assignment-spark-04")
  val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(2))
  val Conf: Config = ConfigFactory.load
  System.setProperty("twitter4j.oauth.consumerKey", Conf.getString("twitter4J.consumerKey.value"))
  System.setProperty("twitter4j.oauth.consumerSecret", Conf.getString("twitter4J.consumerSecret.value"))
  System.setProperty("twitter4j.oauth.accessToken", Conf.getString("twitter4J.accessToken.value"))
  System.setProperty("twitter4j.oauth.accessTokenSecret", Conf.getString("twitter4J.accessSecret.value"))
  val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(streamingContext, None)
  val url = "jdbc:mysql://localhost:3306/sparkTwitter"
  val username = "root"
  val password = "root"
  val hashTags: DStream[String] = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
  hashTags.print()
  val hashTagWithCount = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10)).map { case (topic, count) => (count, topic) }.transform(_.sortByKey(false))
  hashTagWithCount.foreachRDD(hash => hash.take(3).foreach {
    case (count, hashtag) =>
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection(url, username, password)
      val del = conn.prepareStatement("INSERT INTO hashTag (hashtag,count) VALUES (?,?)")
      del.setString(1, hashtag)
      del.setInt(2, count)
      del.executeUpdate
      conn.close()
  })
  streamingContext.start()
  streamingContext.awaitTermination()


}

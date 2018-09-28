package com.github.jmlb23


import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import argonaut._
import Argonaut._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {
    val propsFile = new Properties
    propsFile.load(this.getClass.getClassLoader.getResourceAsStream("twitter.properties"))

    val tSource = new TwitterSource(propsFile)

    val eenv = StreamExecutionEnvironment.getExecutionEnvironment
    val senv = eenv.addSource(tSource)


    senv
        .filter(x => x.contains("created_at"))
        .map(x => JsonParser.parse(x).fold(x => "empty",x => x.field("text").fold("empty")(_.toString())))
        .filter(x => x.contains("love") || x.contains("hate")).map{x =>
      val t1 = if (x.contains("hate")) "hate" else "love"
      (t1,1)
    }.keyBy(0).window(assigner = TumblingProcessingTimeWindows.of(Time.seconds(10))).aggregate(aggregateFunction = new AggregateFunction[(String,Int),(String,Long,Long),(String,Long,Long)] {
      override def createAccumulator(): (String, Long, Long) = ("love/hate",0,0)

      override def add(in: (String, Int), acc: (String, Long, Long)): (String, Long, Long) = if (in._1.equals("love")) (acc._1,acc._2 + 1, acc._3) else (acc._1,acc._2, acc._3 + 1)

      override def getResult(acc: (String, Long, Long)): (String, Long, Long) = acc

      override def merge(acc: (String, Long, Long), acc1: (String, Long, Long)): (String, Long, Long) = (acc1._1,acc._2 + acc1._2, acc._3 + acc1._3)
    }).addSink(t => println(s"10 seconds -----------------------------------\nlove ${t._2}\nhate ${t._3}"))

    eenv.execute()
  }
}


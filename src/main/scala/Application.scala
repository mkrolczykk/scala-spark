package org.example

import task1.TargetDataframe
import task2.ChannelsStatistics
import task3.{CalculateMetrics, WeeklyPurchasesProjection}

import org.apache.log4j.BasicConfigurator

object Application {

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val task: String = args(0)

    if (!checkConfigExists(args)) {
      log.error("Task config file not found - ending program")
      System.exit(0)
    }

    task match {
      case "task1" => TargetDataframe.createTargetDataframe(args.tail)
      case "task2" => ChannelsStatistics.calculateStatistics(args.tail)
      case "task3-metric" => CalculateMetrics.calculateMetric(args.tail)
      case "task3-weekly" => WeeklyPurchasesProjection.createWeeklyPurchasesProjection(args.tail)
      case _ => log.error(s"Invalid argument value '$task'")
    }
  }
}

package com.gilcu2

import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, SparkMainTrait}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

object HelloMain extends SparkMainTrait {

  def process(configValues: ConfigValuesTrait, lineArguments0: LineArgumentValuesTrait)(
    implicit spark: SparkSession): Unit = {

    val lineArguments = lineArguments0.asInstanceOf[CommandParameterValues]

    println(s"Hello ${lineArguments.userName} from Spark")
  }

  def getConfigValues(conf: Config): ConfigValuesTrait = {
    //    val dataDir = conf.getString("DataDir")
    val dataDir = "kk"
    ConfigValues(dataDir)
  }

  def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): LineArgumentValuesTrait = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val logCountsAndTimes = parsedArgs.logCountsAndTimes()
    val userName = parsedArgs.userName()

    CommandParameterValues(logCountsAndTimes, userName)
  }

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val logCountsAndTimes = opt[Boolean]()
    val userName = trailArg[String]()

  }

  case class CommandParameterValues(logCountsAndTimes: Boolean, userName: String) extends LineArgumentValuesTrait

  case class ConfigValues(dataDir: String) extends ConfigValuesTrait

}

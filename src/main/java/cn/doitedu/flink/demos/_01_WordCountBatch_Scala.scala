package cn.doitedu.flink.demos

import org.apache.flink.api.scala.ExecutionEnvironment

object _01_WordCountBatch_Scala {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val source = env.readTextFile("data/wc/wc.txt")

    import org.apache.flink.api.scala._
    source.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()


  }

}

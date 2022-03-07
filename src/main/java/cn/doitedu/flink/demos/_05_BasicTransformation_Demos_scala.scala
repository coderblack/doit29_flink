package cn.doitedu.flink.demos

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object _05_BasicTransformation_Demos_scala {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val source = env.fromCollection(List("a,1,28_b,2,33"))

    source.flatMap(s=>s.split("_"))


  }

}

package sparkApp

/**
  * Created by eva on 4/2/17.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object HIndexCalculator {
  def main(args : Array[String]): Unit = {
    //input
    val inputFile = args(0)
    //output
    val outputFile = args(1)
    val conf = new SparkConf().setMaster("local[*]").setAppName("h-index")
    val sc = new SparkContext(conf)

    val input = sc.textFile(inputFile)

    // read input file line by line and split in commas
    val res  = input.flatMap(line => line.split(",").map(_.trim)).filter(word => !word.isEmpty)
    // output is
    //R2:A
    //R2:A
    //R1:A
    //R2:C
    //R2:B
    //R1:A
    //...
    // group by researcher and count how many citations does each of his/her paper have
    // sort the papers by the number of citations
    val counts = res.map( w => (w ,1)).coalesce(1).reduceByKey((x, y)=> x+y ).sortBy(_._1,true)
    // output is
    //(R1:A,10)
    //(R1:B,8)
    //(R1:C,5)
    //(R1:D,4)
    //(R1:E,3)
    //(R2:A,25)
    //(R2:B,8)
    //(R2:C,5)
    //(R2:D,3)
    //(R2:E,3)
    // use researchers as keys and create a list of values containing their papers
    val res2  = counts.flatMap{a=>
      val list=a._1.split(":")
      val firstTerm=list(0)
      val secondTermAsList=list(1)
      val thirdTermAsList=a._2

      secondTermAsList.map{b=>

        val key=firstTerm
        val value1=secondTermAsList
        val value2=thirdTermAsList

        (key,value1, value2)
      }
    }.groupBy {_._1} map {

      case (k, v) => (k, v map {

        case (k, v1, v2) =>
          (v1, v2)

      })

    }
    // output is
    //(R2,List((A,25), (B,8), (C,5), (D,3), (E,3)))
    //(R1,List((A,10), (B,8), (C,5), (D,4), (E,3)))

    // create an index with the position of each paper in the list
    val res3 = res2.map(t => (t._1, t._2.zipWithIndex))
    // output is
    //(R2,List(((A,25),0), ((B,8),1), ((C,5),2), ((D,3),3), ((E,3),4)))
    //(R1,List(((A,10),0), ((B,8),1), ((C,5),2), ((D,4),3), ((E,3),4)))

    // for each researcher return the minimum position where "number of citations" >= "position"
    val res4 = res3.map{case (k,v)=> (k, v map {

      case ((k, v1), v2) =>

        if(  v1 <= v2 ) v2 else 100000

    })}
    // output
    // (R2,List(100000, 100000, 100000, 3, 4))
    // (R1,List(100000, 100000, 100000, 100000, 4))

    // return the min value for each list
    val res5 = res4.map(t => (t._1, t._2.min)).sortBy(_._1,true)
    // output
    //(R1,4)
    //(R2,3)

    res5.saveAsTextFile(outputFile)


  }


}


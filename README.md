# h-index-spark-

# Big Data Systems 2017, First Spark Assignment

In this assignment, you will use Spark to calculate the h-index.
 https://en.wikipedia.org/wiki/H-index

# h-index

The h-index is an indicator of a researcher's productivity and impact. For a given researcher, we count all the citations to their work. Then we put them in descending order. The h-index is the position, in the order we created, of the last publication whose citation count is greater than the position.

A nice example is given in the Wikipedia article mentioned above. Say we have two researchers, R1R1 and R2R2. Both of them happen to have five publications each, to which we will refer with Ri,jRi,j. For researcher R1R1 we count the citations, order then, and we get:

R1,1=10,R1,2=8,R1,3=5,R1,4=4,R1,5=3R1,1=10,R1,2=8,R1,3=5,R1,4=4,R1,5=3

Similarly, for researcher R2R2 we get:

R2,1=25,R2,2=8,R2,3=5,R2,4=3,R2,5=3R2,1=25,R2,2=8,R2,3=5,R2,4=3,R2,5=3

Then R1R1 has an h-index of 4, because the fourth publication has 4 citations and the fifth has 3. In the same way, R2R2 has an h-index of 3, because the fourth publication has only 3 citations.

Your program will work with files of the form:

R2:A, R2:A, R1:A, R2:C, R2:B, R1:A, R2:C, R2:E, R2:D, R2:D, R1:A, R2:E, R1:D, R2:B
R1:B, R1:A, R1:B, R2:C, R2:B, R2:A, R1:D, R1:B, R1:D, R2:B, R1:A, R1:B
R2:A, R2:A, R1:B, R2:A, R1:C, R2:A, R2:A, R2:A, R2:C, R1:B, R2:A, R2:A, R2:D
R1:E, R2:C, R2:E, R1:A, R1:B, R1:A, R2:A, R2:B, R2:B, R2:A, R2:A, R1:C
R1:C, R2:A, R1:C, R2:A, R2:A, R2:A, R1:E, R2:A, R1:E, R2:B
R1:A, R2:A, R1:D, R2:A, R2:A, R2:A, R1:A, R1:B, R2:A, R1:A, R2:A, R2:B, R1:C

The lines have records separated by commas. Each record mention a researcher and a publication: that is, each record is a citation. Each publication is identifiable by researcher, so R2:A and R1:A are two different publications. Your purpose is to write a Spark program that takes such files and produces an output of the form:

R1 4
R2 3

That is, each line should contain a researcher and their h-index. The researchers should be sorted.

# Solution

```scala
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
```

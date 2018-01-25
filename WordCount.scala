package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object WordCount {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("...")
    
    // Split into words separated by a space character
    val wordCounts = input.flatMap(line => line.split(" "))
                          .map(word => (word, 1)
                          .reduceByKey(_ + _)
    
    // Print the results.
    wordCounts.foreach(println)
    // Save the results                           
    wordCounts.saveAsTextFile(...)
  }
  
}

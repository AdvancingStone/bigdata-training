package com.blue.others

import com.blue.SparkContext
import org.apache.spark.storage.StorageLevel

/**
 * These functions can be used to adjust the storage level of a RDD.
 * When freeing up memory, Spark will use the storage level identifier to decide which partitions should be kept.
 * The parameterless variants persist() and cache() are just abbreviations for persist(StorageLevel.MEMORY_ONLY).
 * (Warning: Once the storage level has been changed, it cannot be changed again!)
 */
object PersistAndCache extends SparkContext with App {

  val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
  val storageLevel: StorageLevel = c.getStorageLevel
  println(storageLevel)
  //  StorageLevel(1 replicas)

  c.cache()
  println(c.getStorageLevel)
  //  StorageLevel(memory, deserialized, 1 replicas)
}

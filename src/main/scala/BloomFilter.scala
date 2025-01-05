package edu.najah.bigdata

import scala.util.hashing.MurmurHash3

/**
  * Bloom filter implementation
  *
  * @param size The size of the bits array
  * @param hashFunctionsCount The number of hash functions to use
  */
class BloomFilter(size: Int, hashFunctionsCount: Int) extends Serializable {
  private val bits = new Array[Boolean](size)
  private val hashes = (0 until hashFunctionsCount).map {
    seed => (text: String) => Math.abs(MurmurHash3.stringHash(text, seed) % size)
  }

  /**
    * Sets the text as seen
    *
    * @param item String text to set in the filter as seen
    */
  def setAsSeen(item: String): Unit = {
    hashes.foreach(h => {
      bits(h(item)) = true
    })
  }

  /**
    * Checks if the item has been seen before.
    *
    * @param item The item to check
    * @return true if the item has been seen; false otherwise
    */
  def hasSeenBefore(item: String): Boolean = {
     hashes.forall(h => bits(h(item)))
  }
}

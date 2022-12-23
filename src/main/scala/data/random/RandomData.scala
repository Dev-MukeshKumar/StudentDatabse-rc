package data.random

import scala.annotation.tailrec
import scala.util.Random

object RandomData {

  //utilities
  private val alpha = "abcdefghijklmnopqrstuvwxyz"

  //generates random number between the range given(inclusive)
  def randomNumberBetween(start:Int,end:Int):Int = start + Random.nextInt((end-start)+1)

  @tailrec
  def randomName(n:Int,result:String=""):String = {
    if(result.length >= n) result.capitalize
    else randomName(n, result+""+alpha.charAt(randomNumberBetween(0,25)))
  }

}

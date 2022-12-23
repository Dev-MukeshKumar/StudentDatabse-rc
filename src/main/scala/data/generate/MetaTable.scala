package data.generate

import data.models._

import scala.annotation.tailrec

object MetaTable {

  val subjectsList = List(
    "Computer Science",
    "Maths",
    "Physics",
    "Chemistry",
    "Botany",
    "Zoology",
    "Business maths",
    "Economics",
    "Commerce",
    "Statistics"
  )

  private val classesList = List("XI","XII")

  @tailrec
  def groups(i:Int=1,result:Seq[Group]=Seq()):Seq[Group] = {
    if(i>=4) result
    else groups(i+1,result :+ Group(Option(i),Option(s"group $i")))
  }

  @tailrec
  def subjects(i:Int=0,result:Seq[Subject]=Seq()):Seq[Subject] = {
    if(i>=subjectsList.length) result
    else subjects(i+1,result :+ Subject(Option(i+1),Option(subjectsList(i))))
  }

  @tailrec
  def classes(i:Int=1,result:Seq[Class]=Seq()):Seq[Class] = {
    if(i>=3) result
    else classes(i+1,result :+ Class(Option(i),Option(classesList(i-1))))
  }

  @tailrec
  def tests(i:Int=1,result:Seq[Test]=Seq()):Seq[Test] = {
    if(i>=4) result
    else tests(i+1, result :+ Test(Option(i),Option(s"Test $i")))
  }

  def classGroupMapper():Seq[ClassGroupMapper] = Seq(1,2).flatMap(x => Seq(1,2,3).map( y => ClassGroupMapper(Option(x),Option(y))))

  val subjectGroupMapper = Seq(1,2,3,4).map( x => SubjectGroupMapper(Option(1),Option(x))) ++  Seq(5,6,3,4).map( x => SubjectGroupMapper(Option(2),Option(x))) ++ Seq(7,8,9,10).map(x => SubjectGroupMapper(Option(3),Option(x)))

}
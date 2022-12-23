package data.generate

import data.generate.MetaTable.subjectGroupMapper
import data.models._
import data.random.RandomData._

import scala.annotation.tailrec

object MainTable {

  //table data list generation
  @tailrec
  def studentsTableList(i:Int=1,result:Seq[Student]=Seq()): Seq[Student] = {
    if(i>900) result
    else{
      val groupId = getGroup(i)
      val classId = getClass(i)
      studentsTableList(i+1,result:+ Student(Option(groupId),Option(classId),Option(i),Option(randomName(8)),Option(randomName(8))))
    }
  }

  def marksTableList(studentsData:Seq[Student]): Seq[Mark] = studentsData.flatMap(student => {
      getSubjects(student.group_id.get)
        .map( subjectId => Mark(student.group_id,student.class_id,Option(subjectId),student.student_id,getTestMarksMap()))
  })

  //utilities
  private def getGroup(i: Int): Int = if ((i / 150.toDouble).ceil > 3) (i / 150.toDouble).ceil.toInt - 3 else (i / 150.toDouble).ceil.toInt

  private def getClass(i: Int): Int = if ((i / 450.toDouble).ceil > 1) 2 else 1

  private def getSubjects(group_id:Int):Seq[Int] = subjectGroupMapper.filter(x => x.group_id.get == group_id).map(x => x.subject_id.get)

  private def getTestMarksMap():Option[Map[Int,Option[Double]]] = Option(Map(1->Option(randomNumberBetween(10,100).toDouble),2->Option(randomNumberBetween(10,100).toDouble),3->Option(randomNumberBetween(10,100).toDouble)))
}

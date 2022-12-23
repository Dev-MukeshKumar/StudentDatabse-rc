package data.models

case class Mark(group_id:Option[Int], class_id:Option[Int], subject_id:Option[Int], student_id: Option[Int], test_marks: Option[Map[Int,Option[Double]]])

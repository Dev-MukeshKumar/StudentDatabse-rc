package data.models

import java.time.LocalDate

case class StudentHistory(group_id: Option[Int], class_id: Option[Int], date_of_leaving: Option[LocalDate], student_id: Option[Int], firstname: Option[String], lastname:Option[String])
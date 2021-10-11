package org.example

trait Task {
  val appConfig = "app-config"
  val taskId: String

  def checkTaskInput(args: Array[String]): Unit
}

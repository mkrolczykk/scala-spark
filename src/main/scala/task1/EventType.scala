package org.example
package task1

object EventType extends Enumeration {
  type EventType = Value

  val appOpen: task1.EventType.Value = Value("app_open")
  val searchProduct: task1.EventType.Value = Value("search_product")
  val viewProductDetails: task1.EventType.Value = Value("view_product_details")
  val purchase: task1.EventType.Value = Value("purchase")
  val appClose: task1.EventType.Value = Value("app_close")
}

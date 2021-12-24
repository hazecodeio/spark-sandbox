package org.hsmak._02_high_level_abstractions.dataset

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

object CreateDSFromDF extends App {

  //turn off Logging
  Configurator.setLevel("org.apache.spark", Level.OFF)

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/NW"
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("DatasetRunner")
    .getOrCreate()

  import CaseClasses._
  import spark.implicits._

  /* ******************************************************
    * ############ Creating SparkSession ###########
    * ***************************************************** */

  val employeesDS = spark.read
    .option("header", "true")
    .csv(s"$base_data_dir/NW-Employees.csv")
    .as[Employee] // This will create a typed DataFrame, aka DataSet

  import spark.implicits._

  val ordersDS = spark.read
    .option("header", "true")
    .csv(s"$base_data_dir/NW-Orders.csv")
    .as[Order] // This will create a typed DataFrame, aka DataSet

  val orderDetailsDS = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/NW-Order-Details.csv")
    .as[OrderDetails] // This will create a typed DataFrame, aka DataSet

  val productsDS = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/NW-Products.csv")
    .as[Product] // This will create a typed DataFrame, aka DataSet


  ////////////////////////////////////////////

  println("Employees has " + employeesDS.count() + " rows")
  employeesDS.show(5)
  println(employeesDS.head())
  employeesDS.dtypes.foreach(println) // verify column types


  //Orders
  //  val ordersDS = ordersDF.as[Order]

  println("Orders has " + ordersDS.count() + " rows")
  ordersDS.show(5)
  println(ordersDS.head())
  ordersDS.dtypes.foreach(println)


  //OrderDetails
  //  val orderDetailsDS = orderDetailsDF.as[OrderDetails]

  println("Order Details has " + orderDetailsDS.count() + " rows")
  orderDetailsDS.show(5)
  println(orderDetailsDS.head())
  orderDetailsDS.dtypes.foreach(println) // verify column types


  //Products
  //  val productsDS = productsDF.as[Product]

  println("Order Details has " + productsDS.count() + " rows")
  productsDS.show(5)
  println(productsDS.head())
  productsDS.dtypes.foreach(println) // verify column types
}

object CaseClasses {

  case class Employee(EmployeeID: String,
                      LastName: String,
                      FirstName: String,
                      Title: String,
                      BirthDate: String,
                      HireDate: String,
                      City: String,
                      State: String,
                      Zip: String,
                      Country: String,
                      ReportsTo: String)

  case class Order(OrderID: String,
                   CustomerID: String,
                   EmployeeID: String,
                   OrderDate: String,
                   ShipCountry: String)

  case class OrderDetails(OrderID: String,
                          ProductID: String,
                          UnitPrice: Double,
                          Qty: Int,
                          Discount: Double)

  case class Product(ProductID: String,
                     ProductName: String,
                     UnitPrice: Double,
                     UnitsInStock: Int,
                     UnitsOnOrder: Int,
                     ReorderLevel: Int,
                     Discontinued: Int)

}
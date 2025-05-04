package org.haze._03_sql.example

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

object DatasetSQLOpsOnNW extends App {

  //turn off Logging
  Configurator.setLevel("org.apache.spark", Level.OFF)


  case class Employee(EmployeeID: String,
                      LastName: String, FirstName: String, Title: String,
                      BirthDate: String, HireDate: String,
                      City: String, State: String, Zip: String, Country: String,
                      ReportsTo: String)

  case class Order(OrderID: String, CustomerID: String, EmployeeID: String,
                   OrderDate: String, ShipCountry: String)

  case class OrderDetails(OrderID: String, ProductID: String, UnitPrice: Double,
                          Qty: Int, Discount: Double)

  case class Product(ProductID: String, ProductName: String, UnitPrice: Double, UnitsInStock: Int, UnitsOnOrder: Int, ReorderLevel: Int, Discontinued: Int)


  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/NW"


  /* ******************************************************
    * ############ Creating SparkSession ###########
    * ***************************************************** */

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("DatasetRunner")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()


  import spark.implicits._


  /* ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ***************************************************** */


  val employeesDF = spark.read
    .option("header", "true")
    .csv(s"$base_data_dir/NW-Employees.csv")


  val ordersDF = spark.read
    .option("header", "true")
    .csv(s"$base_data_dir/NW-Orders.csv")


  val orderDetailsDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/NW-Order-Details.csv")


  val productsDF = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/NW-Products.csv")


  /* ****************************************************
    * ############ Creating Datasets from DF ###########
    * *************************************************** */


  //Employees
  val employeesDS = employeesDF.as[Employee] // binding to a type

  println("Employees has " + employeesDS.count() + " rows")
  employeesDS.show(5)
  println(employeesDS.head())
  employeesDS.dtypes.foreach(println) // verify column types


  //Orders
  val ordersDS = ordersDF.as[Order]

  println("Orders has " + ordersDS.count() + " rows")
  ordersDS.show(5)
  println(ordersDS.head())
  ordersDS.dtypes.foreach(println)


  //OrderDetails
  val orderDetailsDS = orderDetailsDF.as[OrderDetails]

  println("Order Details has " + orderDetailsDS.count() + " rows")
  orderDetailsDS.show(5)
  println(orderDetailsDS.head())
  orderDetailsDS.dtypes.foreach(println) // verify column types


  //Products
  val productsDS = productsDF.as[Product]

  println("Order Details has " + productsDS.count() + " rows")
  productsDS.show(5)
  println(productsDS.head())
  productsDS.dtypes.foreach(println) // verify column types


  /* ******************************************************
    * ################# Creating Views/Tables ##############
    * ***************************************************** */


  employeesDS.createOrReplaceTempView("EmployeesTable")
  ordersDS.createOrReplaceTempView("OrdersTable")
  orderDetailsDS.createOrReplaceTempView("OrderDetailsTable")
  productsDS.createOrReplaceTempView("ProductsTable")


  /* ******************************************************
    * ################# SQL Operations #####################
    * ****************************************************** */


  //Joining two tables
  val Orders_JOIN_OrderDetails = spark.sql(

    """SELECT OrderDetailsTable.OrderID, ShipCountry, UnitPrice, Qty, Discount
      |FROM OrdersTable
      |INNER JOIN OrderDetailsTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID""".stripMargin)

  Orders_JOIN_OrderDetails.show(10)
  Orders_JOIN_OrderDetails.head(3).foreach(println)


  // Sales By Country

  val Sales_GROUPEDBY_ShipCountry = spark.sql(

    """SELECT ShipCountry, SUM(OrderDetailsTable.UnitPrice * Qty * Discount) AS ProductSales
      |FROM OrdersTable
      |INNER JOIN OrderDetailsTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID
      |GROUP BY ShipCountry""".stripMargin)

  println(Sales_GROUPEDBY_ShipCountry.count())
  Sales_GROUPEDBY_ShipCountry.show(100)
  Sales_GROUPEDBY_ShipCountry.head(3).foreach(println)

  Sales_GROUPEDBY_ShipCountry.orderBy($"ProductSales".desc).show(10) // Top 10 by Sales

  // We could use the already joined tables in 'Orders_JOIN_OrderDetails' instead of rejoining again

  import org.apache.spark.sql.functions.sum

  Orders_JOIN_OrderDetails
    .groupBy("ShipCountry")
    .agg(sum($"UnitPrice" * $"Qty" * $"Discount").as("ProductSales"))
    .show
  Orders_JOIN_OrderDetails.createOrReplaceTempView("Orders_JOIN_OrderDetails") // Alternatively, we could create a temp view for it
  spark.sql(
    """
      |SELECT ShipCountry, SUM(UnitPrice * Qty * Discount) AS ProductSales
      |FROM Orders_JOIN_OrderDetails
      |GROUP BY ShipCountry""".stripMargin)
    .show


  //Sales GroupedBy Products
  val Sales_GROUPEDBY_Products = spark.sql(

    """SELECT ProductName, SUM(OrderDetailsTable.Qty) AS ProductSales
      |FROM ProductsTable
      |
      |INNER JOIN OrderDetailsTable ON ProductsTable.ProductID = OrderDetailsTable.ProductID
      |
      |GROUP BY ProductName
      |ORDER BY ProductSales DESC""".stripMargin)

  Sales_GROUPEDBY_Products.show

  /**
    * for date/timestamp ops, refer to:
    *   - https://spark.apache.org/docs/latest/api/sql/index.html#timestamp
    *   - https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    *
    * ```
    * sql> trunc(to_date(OrdersTable.OrderDate, 'dd/mm/yy'), 'yyyy')
    * ```
    *
    */


  //Sales GroupedBy Products for 1997
  val Sales_GROUPEDBY_Products_1997 = spark.sql(

    """SELECT ProductName, SUM(OrderDetailsTable.Qty) AS ProductSales_97
      |FROM ProductsTable
      |
      |INNER JOIN OrderDetailsTable ON ProductsTable.ProductID = OrderDetailsTable.ProductID
      |INNER JOIN OrdersTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID
      |
      |WHERE trunc(to_date(OrdersTable.OrderDate, 'dd/mm/yy'), 'yyyy') = trunc('1997', 'yyyy')
      |
      |GROUP BY ProductName
      |ORDER BY ProductSales_97 DESC""".stripMargin)

  Sales_GROUPEDBY_Products_1997.show
  /*

      +--------------------+---------------+
      |         ProductName|ProductSales_97|
      +--------------------+---------------+
      | GnocchidinonnaAlice|            921|
      | RacletteCourdavault|            752|
      |    GorgonzolaTelino|            672|
      |    CamembertPierrot|            665|
      | Rh�nbr�uKlosterbier|            630|
      |      BostonCrabMeat|            626|
      |    SirRodneysScones|            610|
      |             Pavlova|            571|
      |JacksNewEnglandCl...|            549|
      |         AliceMutton|            527|
      | ManjimupDriedApples|            501|
      |        Lakkalik��ri|            497|
      |           Tourti�re|            494|
      |        Tarteausucre|            482|
      |LouisianaFieryHot...|            470|
      |OriginalFrankfurt...|            452|
      |SingaporeanHokkie...|            451|
      |               Chang|            435|
      |    Gudbrandsdalsost|            430|
      |   Guaran�Fant�stica|            421|
      +--------------------+---------------+
      only showing top 20 rows

   */
}

package org.hsmak._03_sql.example

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

object DataWranglingOnNW extends App {

  //turn off Logging
  Configurator.setLevel("org.apache.spark", Level.OFF)

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/NW"


  /* ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("DataWranglingOnNW")
    .getOrCreate()


  import spark.implicits._


  /* ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  val ordersDF = spark.read
    .option("header", "true")
    .csv(s"$base_data_dir/NW-Orders.csv")


  val orderDetailsDF = spark.read
    .option("header", "true").
    option("inferSchema", "true")
    .csv(s"$base_data_dir/NW-Order-Details.csv")


  /**
    * This SparkApp is addressing the following questions:
    *
    * 1. How many orders were placed by each customer?
    * 2. How many orders were placed in each country?
    * 3. How many orders were placed per month & year?
    * 4. What is the total number of sales for each customer per year?
    * 5. What is the average order by customer per year?
    */


  /* *************************************************************************
    * ############ Q1. How many orders were placed by each customer? ###########
    * *************************************************************************/

  val orderByCustomer = ordersDF.groupBy($"CustomerID").count
  orderByCustomer.sort($"count".desc).show(5)
  // Alternatively using aggregate functions

  import org.apache.spark.sql.functions._

  ordersDF.groupBy($"CustomerID").agg(count($"CustomerID").as("count")).sort($"count".desc).show(6)


  /* ************************************************************************
    * ############ Q2. How many orders were placed in each country? ###########
    * ************************************************************************/

  val orderByCountry = ordersDF.groupBy("ShipCountry").count()
  orderByCountry.sort(orderByCountry("count").desc).show(5)

  // Alternatively using aggregate functions

  import org.apache.spark.sql.functions._

  ordersDF.groupBy($"ShipCountry")
    .agg(count($"ShipCountry").as("count"))
    .sort($"count".desc)
    .show(6)


  /* ***************************************
    * ############ Data Wrangling ###########
    * ***************************************/

  /**
    * Q3 - Q5 will need Data Wrangling before the answers are attempted. Data Wrangling will be performed as follows:
    *
    * 1. Add TotalPrice/Order column to the OrdersDF
    * 1.1. Add OrderPrice to OrderDetails by performing the arithmetic calculation
    * 1.2. Aggregate TotalPrice by OrderID
    * 1.3. Join OrderDetails & Orders so the new table will have TotalPrice
    * 1.4. Perform null-check on TotalPrice column
    * 2. Add a "Date" column
    * 3. Extract "Month" & "Year" from the "Date" column
    */

  //# 1.1. Add OrderPrice to OrderDetails by performing the arithmetic calculation
  val ordersWithPrice = orderDetailsDF.select(
    $"OrderID",
    (($"UnitPrice" * $"Qty") - (($"UnitPrice" * $"Qty") * $"Discount")).as("OrderPrice"))
  ordersWithPrice.show(5)

  //# 1.2. Aggregate TotalPrice by OrderID
  val ordersWithTotalPrice = ordersWithPrice.groupBy($"OrderID").agg(sum($"OrderPrice").as("TotalPrice"))
  ordersWithTotalPrice.sort($"OrderID".asc).show


  //# 1.3. Join OrderDetails & Orders so the new table will have TotalPrice

  /**
    * This is INNER JOIN on OrderID
    */
  val orders_total_joined = ordersDF
    .join(ordersWithTotalPrice, ordersDF("OrderID") === ordersWithTotalPrice("OrderID"), "inner")
    .select(
      ordersDF("OrderID"), // Pick one column out of the two joined tables
      $"CustomerID",
      $"EmployeeID",
      $"OrderDate",
      $"ShipCountry",
      $"TotalPrice")

  orders_total_joined.sort("CustomerID").show

  //# 1.4. Perform null-check on TotalPrice column.
  // Empty records means no record with TotalPrice as null.
  orders_total_joined.filter($"TotalPrice".isNull).show


  //# 2. Add a "Date" column
  //# 3. Extract "Month" & "Year" from the "Date" column
  val orders_totalPrice_Date = orders_total_joined
    .withColumn("Date", to_date($"OrderDate", "MM/dd/yy")) // convert Date from type String to type Date
    .withColumn("Month", month($"Date")) // extract Month from Date
    .withColumn("Year", year($"Date")) // extract Year from Date
  orders_totalPrice_Date.show(30)
  orders_totalPrice_Date.printSchema()


  //testing
  val totalOrdersByYear = orders_totalPrice_Date.groupBy($"Year").agg(sum($"TotalPrice"))
  totalOrdersByYear.sort($"Year").show(30)


  /* ********************************************************************
    * ######### Q3. How many orders were placed per month & year? ########
    * ********************************************************************/

  val totalOrdersByYearMonth = orders_totalPrice_Date.groupBy($"Year", $"Month").agg(sum($"TotalPrice"))
  totalOrdersByYearMonth.sort($"Year", $"Month").show(30)


  /* ******************************************************************************************
    * ############ Q4. What is the total number of sales for each customer per year? ###########
    * ******************************************************************************************/

  val totalOrdersByCustomerYear = orders_totalPrice_Date.groupBy($"CustomerID", $"Year").agg(sum($"TotalPrice"))
  totalOrdersByCustomerYear.sort($"CustomerID", $"Year").show(30)


  /* ****************************************************************************
    * ############ Q5. What is the average order by customer per year? ###########
    * ****************************************************************************/

  val avgOrdersByCustomerYear = orders_totalPrice_Date.groupBy($"CustomerID", $"Year").agg(avg($"TotalPrice"))
  avgOrdersByCustomerYear.sort($"CustomerID", $"Year").show(30)


  /* *****************************************************
    * ############ Q6. Average order by customer ##########
    * *****************************************************/

  val avgOrdersByCustomer = orders_totalPrice_Date.groupBy($"CustomerID").agg(avg($"TotalPrice").as("AvgTotalPrice"))
  avgOrdersByCustomer.sort($"AvgTotalPrice".desc).show(30)
}

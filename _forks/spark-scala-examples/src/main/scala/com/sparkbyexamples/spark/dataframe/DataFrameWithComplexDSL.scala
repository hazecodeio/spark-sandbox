package com.sparkbyexamples.spark.dataframe

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
case class Employee(firstName:String,lastName:String, email:String,salary:Int)
case class Department(id:Int,name:String)
case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])

/**
  * Observations:
  *     - Use "struct_name.*"     -> for nested structs
  *     - Use "explode(col_name)" -> for flattening an Array
  */
object DataFrameWithDSL2 extends MyContext{

  def main(args: Array[String]): Unit = {

    val department1 = Department(123456, "Computer Science")
    val department2 = Department(789012, "Mechanical Engineering")
    val department3 = Department(345678, "Theater and Drama")
    val department4 = Department(901234, "Indoor Recreation")

    //Create the Employees

    val employee1 = Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
    val employee2 = Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
    val employee3 = Employee("matei", "", "no-reply@waterloo.edu", 140000)
    val employee4 = Employee("", "wendell", "no-reply@berkeley.edu", 160000)

    //Create the DepartmentWithEmployees instances from Departments and Employees
    val departmentWithEmployees1 = DepartmentWithEmployees(department1, List(employee1, employee2))
    val departmentWithEmployees2 = DepartmentWithEmployees(department2, List(employee3, employee4))
    val departmentWithEmployees3 = DepartmentWithEmployees(department3, List(employee1, employee4))
    val departmentWithEmployees4 = DepartmentWithEmployees(department4, List(employee2, employee3))

    val data1 = Seq(departmentWithEmployees1,departmentWithEmployees2)

    val data2 = Seq(departmentWithEmployees3,departmentWithEmployees4)

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //val df = spark.createDataFrame(data1)
    //val df2 = spark.createDataFrame(data2)
    // Alternative to the previous two lines
    import spark.implicits._
    val df = data1.toDF
    val df2 = data2.toDF


    //union
    val finalDF = df.union(df2)
    finalDF.printSchema()
    finalDF.show(false)

    finalDF.select("department.*").printSchema()
    finalDF.select(explode(col("employees"))).show(false)
    finalDF.select(explode(col("employees"))).select("col.*").show(false) // default column name after exploding is "col"

    // All together
    finalDF.select($"department", explode($"employees").as("empls")) // exploding the Array
      .select($"department.*", $"empls.*") // expanding; flattening, the Nested StructTypes
      .show(false)

  }
}

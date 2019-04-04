package com.pavanpkulkarni.workspace

/*

Objective :
1. Write a Scala program to read a JSON file with 6 fields {Customer ID, Customer Name, Product, Product ID, Price and Purchase Date}. Read the file using this schema and write the same in CSV format (comma-delimited).
2. In continuation to above question, now create a new file using Scala in Parquet format by including only 5 fields. {Customer ID, Customer Name, Product, Price and Purchase Date}

 */

import org.apache.spark.sql.{SaveMode, SparkSession}

object ScalaJSONToCSV {

  def main(args: Array[String]): Unit = {

    //Initialize the SparkSession
    val spark = SparkSession
      .builder()
      .appName("JsonToCSVConverter")
      .master("local[2]")
      .getOrCreate()

    val resourcesDir = "/Users/pavanpkulkarni/Documents/workspace/ScalaTestWorkspace/src/main/resources/"

    /*
    Solution to part 1 --> Write a Scala program to read a JSON file with 6 fields {Customer ID, Customer Name, Product, Product ID, Price and Purchase Date}. Read the file using this schema and write the same in CSV format (comma-delimited).

    Pesudo Algorithm:
    1. Read JSON file as Dataframe.
    2. Write Dataframe to CSV
     */
    //Read the JSON file
    val inputJSONDF = spark.read.json(resourcesDir + "input.json")

    //Display JSON file
    inputJSONDF.show()

    //Write JSON to CSV
    inputJSONDF
      .write
      .mode(SaveMode.Overwrite)
      .csv(resourcesDir + "output_csv")

    /*
    Solution to part 2 --> In continuation to above question, now create a new file using Scala in Parquet format by including only 5 fields. {Customer ID, Customer Name, Product, Price and Purchase Date}

    Pesudo Algorithm:
    1. Rename to columns to remove any white spaces from column name
    2. Select the required columns (5) from the dataframe
    3. Write the dataframe as parquet.
     */

    val parquetDF = inputJSONDF
      .withColumnRenamed("Customer ID", "Customer_ID"  )
      .withColumnRenamed("Customer Name", "Customer_Name"  )
      .withColumnRenamed("Purchase Date", "Purchase_Date"  )
      .select("Customer_ID", "Customer_Name", "Price","Product", "Purchase_Date")

    parquetDF.show

    parquetDF.write.mode(SaveMode.Overwrite).parquet(resourcesDir + "output_parquet")



  }

}

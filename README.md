# Coding Challenge

## Objectives:
1. Write a Scala program to read a JSON file with 6 fields {Customer ID, Customer Name, Product, Product ID, Price and Purchase Date}. Read the file using this schema and write the same in CSV format (comma-delimited).
2. In continuation to above question, now create a new file using Scala in Parquet format by including only 5 fields. {Customer ID, Customer Name, Product, Price and Purchase Date}

## Source Code

The source code can be found here [ScalaJSONToCSV.scala](https://github.com/pavanpkulkarni/ScalaTestWorkspace/blob/master/src/main/scala/com/pavanpkulkarni/workspace/ScalaJSONToCSV.scala)

## Solution to Part 1
1. A JSON file is [input.json](https://github.com/pavanpkulkarni/ScalaTestWorkspace/blob/master/src/main/resources/input.json) is created under resources directory with sample data 

        {"Customer ID":"001", "Customer Name":"John Doe", "Product":"Dart Board", "Product ID":"123", "Price":70, "Purchase Date":"2019-04-04"},
        {"Customer ID":"002", "Customer Name":"Jane Doe", "Product":"Keyboard", "Product ID":"124", "Price":49.99, "Purchase Date":"2019-04-03"},
        {"Customer ID":"003", "Customer Name":"Hercule Poirot", "Product":"Magnifying Glass", "Product ID":"125", "Price":249.99, "Purchase Date":"2019-04-02"},
        {"Customer ID":"004", "Customer Name":"Frank Underwood", "Product":"Water Rower", "Product ID":"323", "Price":3999.99, "Purchase Date":"2019-04-02"},
        {"Customer ID":"005", "Customer Name":"Pika Achu", "Product":"Duracell Batteries", "Product ID":"130", "Price":15.99, "Purchase Date":"2019-04-01"}
    
2. Using `SparkSession` instance read the JSON file into a DataFrame (inputJSONDF).
3. Display the DataFrame (inputJSONDF) for validation (only for devs)
4. Write the DataFrame (inputJSONDF) as CSV to any desired location. For this coding challenge, I have written the file to [output_csv](https://github.com/pavanpkulkarni/ScalaTestWorkspace/tree/master/src/main/resources/output_csv) directory

[CSV Output File](https://github.com/pavanpkulkarni/ScalaTestWorkspace/blob/master/src/main/resources/output_csv/part-00000-7c1897c2-1e72-42aa-ac81-e2baf68860b6-c000.csv)

## Solution to Part 2

1. Using the above created DataFrame (inputJSONDF). 
2. Rename all the fields that have blank space in their field name. This is to conform and abide by the parquet file naming convention. 
3. Select only the required columns and create a new dataframe (parquetDF) with only the required data.
4. Write the newly create  dataframe (parquetDF) as parquet file. For this coding challenge, I have written the file to [output_parquet](https://github.com/pavanpkulkarni/ScalaTestWorkspace/tree/master/src/main/resources/output_parquet) directory. 
 
[Parquet Output File](https://github.com/pavanpkulkarni/ScalaTestWorkspace/blob/master/src/main/resources/output_parquet/part-00000-939487d5-a388-40d1-a2ab-e3e19562ce35-c000.snappy.parquet)


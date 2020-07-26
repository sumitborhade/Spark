package com.spark.application1;

import static org.apache.spark.sql.functions.*;

import java.util.Scanner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "F:\\Spark\\Spark\\Utils\\");
		
		SparkSession sparkSession 
				= SparkSession.builder()
					.appName("First Spark App")
					.master("local[3]")
					.getOrCreate();

		Dataset<Row> df = sparkSession.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load("src/main/resources/Sample.csv");
		
		Dataset<Row> df1 = df.select(col("Emp ID"), concat(col("First Name"), lit(" "), col("Last Name")).as("Full Name"), col("Date of Birth"))
				.where(col("Full Name").startsWith("C"))
				.withColumnRenamed("Date of Birth", "DOB")
				.withColumn("DOB", to_date(col("DOB"),"M/d/y"))
				.orderBy(col("Full Name").asc(), col("DOB").asc());

//		Printing the schema
		df1.printSchema();
		df1.show();
//		df1.show((int)df.count(), false);

		Scanner scanner = new Scanner(System.in);
//		Uncomment the code for debugging and checking Spark UI
//		scanner.nextLine();
		
		scanner.close();
		sparkSession.stop();
	}

}
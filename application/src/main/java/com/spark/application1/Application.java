package com.spark.application1;

import java.util.Scanner;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {

	static {
	}

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "F:\\Spark\\Spark\\Utils");
		SparkSession sparkSession = SparkSession.builder().appName("First Spark App").master("local[3]").getOrCreate();

		Dataset<Row> df = sparkSession.read().option("header", "true").option("inferSchema", "true")
				.csv("src/main/resources/Sample.csv");

		Dataset<Row> df1 = df.select("Emp ID", "First Name", "Last Name", "Date of Birth")
				.withColumnRenamed("Date of Birth", "DOB")
				.withColumn("DOB", new Column("DOB")).cas;
		df1.printSchema();
		df1.show();

		Scanner scanner = new Scanner(System.in);
//		scanner.nextLine();
		
		scanner.close();
		sparkSession.stop();
	}

}
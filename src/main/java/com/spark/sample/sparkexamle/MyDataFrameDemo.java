package com.spark.sample.sparkexamle;

import java.util.HashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class MyDataFrameDemo {
		public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Spark DataFrame Demo")
				.setMaster("local");
		SparkContext context = new SparkContext(conf);
		SQLContext sqlContext = new SQLContext(context);
		
		DataFrame df = sqlContext.read().format("com.databricks.spark.csv")
				.option("inferSchema", "true").option("header", "true")
				.option("treatEmptyValuesAsNulls","true").load("emp.csv");

		
		Function<DataFrame, DataFrame> filterNonNullValuesFunc = new Function<DataFrame, DataFrame>() {
			@Override
			public DataFrame apply(DataFrame df) {
				return df.filter(df.col("name").isNotNull()
						.and(df.col("dept").isNotNull()));
			}
		};
		
		Function<DataFrame, DataFrame> filterCSDeptFunc = new Function<DataFrame, DataFrame>() {
			@Override
			public DataFrame apply(DataFrame df) {
				return df.filter(df.col("dept").equalTo("CS"));
			}
		};
	
		Function<DataFrame, DataFrame> chnagekkName = new Function<DataFrame, DataFrame>() {
			@Override
			public DataFrame apply(DataFrame df) {
				return df.select(df.col("id"),
						when(df.col("name").equalTo("kk"),"Pawan").otherwise(df.col("name")).as("name"),
						col("dept"));
			}
		};
	
		BiFunction<DataFrame,DataFrame,DataFrame> joinEmpDept = new BiFunction<DataFrame,DataFrame,DataFrame>()
				{
					@Override
					public DataFrame apply(DataFrame emp, DataFrame dept) {
					DataFrame joinDF= emp.join(dept,emp.col("id").equalTo(dept.col("id")),"left_outer");
                    return joinDF;						
					}
				};
		
		DataFrame filNonNullDF = filterNonNullValuesFunc.apply(df);
		//filNonNullDF.show();
		
		DataFrame csDeptDF = filterCSDeptFunc.apply(filNonNullDF);
		//csDeptDF.show();
		
		DataFrame kkDF=chnagekkName.apply(csDeptDF);
	//	kkDF.show();
		
		DataFrame filterFinalDF=filterNonNullValuesFunc
						.andThen(filterCSDeptFunc)
						.andThen(chnagekkName)
						.apply(df);
		//filterFinalDF.show();
		
		DataFrame dept = sqlContext.read().format("com.databricks.spark.csv")
				.option("inferSchema", "true").option("header", "true")
				.option("treatEmptyValuesAsNulls","true").load("dept.csv");
		
		DataFrame finalETL=joinEmpDept.apply(filterFinalDF,dept);
		finalETL.show();		
		
	}
}

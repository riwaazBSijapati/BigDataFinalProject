package cs523.demo;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;


public class SQLDemo 
{
    public static void main( String[] args ){
    	String inPath = args[0]; //hdfs://localhost/user/cloudera/nba_archive/csv
		String outPath = args[1]; //hdfs://localhost/user/cloudera/nba_archive/parquet
    	JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("sparkSQL").setMaster("local[2]"));
		HiveContext hiveContext = new HiveContext(sc.sc());
		hiveContext.setConf("spark.sql.warehouse.dir", "hdfs://localhost/user/hive/warehouse");
		hiveContext.setConf("hive.metastore.uris", "thrift://localhost:9083");
		int numResults = 10; //Sets row limit of the 2 queries
		
		hiveContext.sql("show tables");
		
		System.out.println("Loading hive table into memory");
		DataFrame df = hiveContext.read().load(outPath).as("common_player_info");
		
//		System.out.println("Writing a test hive query: SELECT display_first_last, school, country, last_affiliation FROM common_player_info where greatest_75_flag = 'Y'");
//		Row[] results = hiveContext.sql("SELECT display_first_last, school, country, last_affiliation FROM common_player_info where greatest_75_flag = 'Y'").limit(numResults).collect();
//		for(int i = 0; i < numResults; i++){
//			System.out.println(results[i]);
//			}
//		
		System.out.println("Writing another query programatically using Spark Java API");
		List<Row> list = df.takeAsList(numResults);		
		for(Row r : list){
			System.out.println(r);
		}
		
		System.out.println(df.describe());
    }
}

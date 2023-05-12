package cs523.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class PlayersExport {
	
	  public static void main( String[] args ) {
	  String inPath = "hdfs://localhost/user/cloudera/nba_archive/csv/common_player_info.csv";
	  
	  JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("sparkSQL").setMaster("local[4]"));
	  HiveContext sqlContext = new HiveContext(sc.sc());
	  sqlContext.setConf("spark.sql.warehouse.dir", "hdfs://localhost/user/hive/warehouse");
	  sqlContext.setConf("hive.metastore.uris", "thrift://localhost:9083");
	  
	  DataFrame df = sqlContext.read()
		.format("com.databricks.spark.csv")
		.option("header", "true")
		.option("inferSchema", "true")
		.load(inPath);
	  
			
	  df = df.select("display_first_last", "school", "country", "last_affiliation", "birthdate", "height", "weight", 
						 "jersey", "position", "rosterstatus", "team_name", "team_city", "from_year", "to_year", "greatest_75_flag")
		.orderBy("from_year");	 
			df.show();
			sqlContext.sql("DROP TABLE IF EXISTS players");
			df.write().mode(SaveMode.Overwrite).saveAsTable("players");
		
			sc.close();
		
	  }
}

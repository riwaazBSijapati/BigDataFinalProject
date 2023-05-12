package cs523.hiveloader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class ViewLoader {

	  public static void main( String[] args ) {
		  
		  String inPath = "hdfs://localhost/user/cloudera/nba_archive/csv";
		  String[] neededTables = {"/draft_combine_stats.csv", "/common_player_info.csv", "/game.csv", "/game_info.csv"};
		  String[] viewNames = { //cannot start with _ 
				  "useful_draft_stats",
				  "draftees_per_season",
				  "greatest_75",
				  "useful_game_stats",
				  "schools_of_greatest_75",
				  "best_away_teams",
				  "best_at_home",
				  "most_attended_games"
		  };
		  
		  JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("sparkSQL").setMaster("local[4]"));
		  HiveContext sqlContext = new HiveContext(sc.sc());
		  sqlContext.setConf("spark.sql.warehouse.dir", "hdfs://localhost/user/hive/warehouse");
		  sqlContext.setConf("hive.metastore.uris", "thrift://localhost:9083");
		  DataFrame df = sqlContext.emptyDataFrame();
		 
//		  	//query 1 -> useful_draft_stats
//			  df = sqlContext.read()
//						.format("com.databricks.spark.csv")
//						.option("header", "true")
//						.option("inferSchema", "true")
//						.load(inPath+neededTables[0]);
//			
//			  df = df.select("season", "player_name", "position", "height_wo_shoes", 
//					  "weight", "wingspan", "standing_vertical_leap", "lane_agility_time");
////			  df.registerTempTable(viewNames[0]); //registering each as temp table for joins within this program
//			  df.show();
//			  sqlContext.sql("DROP TABLE IF EXISTS "+viewNames[0]);
//			  df.write().mode(SaveMode.Overwrite).saveAsTable(viewNames[0]);
//			  
//			  //query 1.5 -> draftees_per_season
//			  df = df.groupBy("season").count();
//			  sqlContext.sql("DROP TABLE IF EXISTS "+viewNames[1]);
//			  df.write().mode(SaveMode.Overwrite).saveAsTable(viewNames[1]);
			  
			  //query 2 -> greatest_75
			  df = sqlContext.read()
						.format("com.databricks.spark.csv")
						.option("header", "true")
						.option("inferSchema", "true")
						.load(inPath+neededTables[1]);
			 df = df.filter("greatest_75_flag = 'Y'")
					 .select("display_first_last", "school", "country", "last_affiliation", "birthdate", "height", "weight", 
							 "jersey", "position", "rosterstatus", "team_name", "team_city", "from_year", "to_year")
							 .orderBy("from_year");
//		     df.registerTempTable(viewNames[2]);			 
			 df.show();
			 sqlContext.sql("DROP TABLE IF EXISTS "+viewNames[2]);
			 df.write().mode(SaveMode.Overwrite).saveAsTable(viewNames[2]);
			 
			 
			 //query 2.5 -> schools_of_greatest_75
			 df = df.groupBy("school").count().orderBy(org.apache.spark.sql.functions.desc("count"));
			 df.show();
			 sqlContext.sql("DROP TABLE IF EXISTS "+viewNames[4]);
			 df.write().mode(SaveMode.Overwrite).saveAsTable(viewNames[4]);
			 
			 //query 3 -> useful_game_stats
			 df = sqlContext.read()
						.format("csv")
						.option("header", "true")
						.option("inferSchema", "true")
						.load(inPath+neededTables[2]); //fgm = field goals made, fg3m = 3 pointers made, ftm = free throws made
			 DataFrame game = df;
			 df = df.select("team_name_home", "wl_home", "fgm_home", "fg3m_home", "ftm_home", "pts_home", "team_name_away", "wl_away", "fgm_away", "fg3m_away","ftm_away", "pts_away");
			 df.registerTempTable(viewNames[3]);			 
			 df.show();
			 sqlContext.sql("DROP TABLE IF EXISTS "+viewNames[3]);
//			 df.write().mode(SaveMode.Overwrite).saveAsTable(viewNames[3]);		
			 
//			 //query 3.5 -> "best_away_teams", "best_home_teams"
//			 DataFrame df1 = game.select("team_name_away", "wl_away").where("wl_away = 'Y'")
//					 .groupBy("team_name_away").count()
//					 .orderBy(org.apache.spark.sql.functions.desc("count"));
//			 df1.show();
//			 sqlContext.sql("DROP TABLE IF EXISTS "+viewNames[5]);
//			 df1.write().mode(SaveMode.Overwrite).saveAsTable(viewNames[5]);	
//
//			 //query 3.6 -> "best_at_home"
//			 df = df.select("team_name_home", "wl_home").where("wl_home = 'Y'")
//					 .groupBy("team_name_home").count()
//					 .orderBy(org.apache.spark.sql.functions.desc("count"));
//			 df.show();
//			 sqlContext.sql("DROP TABLE IF EXISTS "+viewNames[6]);
//			 df.write().mode(SaveMode.Overwrite).saveAsTable(viewNames[6]);	
			 
			 //Query 4: join game_info, game, and w/e else to answer most attended games
			 DataFrame game_info = sqlContext.read()
						.format("com.databricks.spark.csv")
						.option("header", "true")
						.option("inferSchema", "true")
						.load(inPath+neededTables[3]);
			 
			 game_info = game_info.filter(game_info.col("attendance").isNotNull())
					 	.join(game, game.col("game_id").equalTo(game_info.col("game_id")))
					 	.drop(game.col("game_date"))
					 	.select("game_date", "attendance","game_time", "team_name_home","team_name_away", "pts_home", "pts_away")
					 	.orderBy(org.apache.spark.sql.functions.desc("attendance"));
			 game_info.show();
			 game_info.write().mode(SaveMode.Overwrite).saveAsTable("most_attended_games");
			 
		  sc.close();
	  }
	
}

package cs523.hiveloader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;


public class App {
	
    public static void main( String[] args ) {
    	
    	if (args.length != 2)
		{
    		System.err.println(args.length+" arguments found:");
			System.err.println("Usage: [generic options] <hdfsInput> <hdfsOutput>");
			
		}else{
    	
    			String inPath = args[0]; //hdfs://localhost/user/cloudera/nba_archive/csv
    			String outPath = args[1]; //hdfs://localhost/user/cloudera/nba_archive/parquet
    			
    			JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("sparkSQL").setMaster("local[4]"));
    			HiveContext sqlContext = new HiveContext(sc.sc());
    			sqlContext.setConf("spark.sql.warehouse.dir", "hdfs://localhost/user/hive/warehouse");
    			sqlContext.setConf("hive.metastore.uris", "thrift://localhost:9083");
    			Configuration configuration = new Configuration();
    			try {
					FileSystem hdfs = FileSystem.get(new URI(inPath), configuration );
					RemoteIterator<LocatedFileStatus> fileStatusListIterator = hdfs.listFiles(
				            new Path(inPath), true);
				    while(fileStatusListIterator.hasNext()){
				        LocatedFileStatus fileStatus = fileStatusListIterator.next();
		    			DataFrame df = sqlContext.read()
		    					.format("com.databricks.spark.csv")
		    					.option("header", "true")
		    					.option("inferSchema", "true")
		    					.load(fileStatus.getPath().toString());
		    			String tableNameRaw = fileStatus.getPath().getName();
		    			String tableName = tableNameRaw.substring(0,tableNameRaw.length()-4);
		    			sqlContext.sql("DROP TABLE IF EXISTS "+tableName);
		    			System.out.println("Overwriting table `"+tableName +"` to Hive");
//		    			df.write().mode(SaveMode.Overwrite).option("path",outPath).saveAsTable(tableName);
		    			df.write().mode(SaveMode.Overwrite).saveAsTable(tableName); //does this one make managed tables?

				    }
				} catch (IOException e) {
					e.printStackTrace();
				} catch (URISyntaxException e) {
					e.printStackTrace();
				} catch(Exception e){
					e.printStackTrace();
				}
 
    			sc.close();
		}
    }
}

package cs523.projectStreamer;

import java.time.LocalDateTime;

public class StreamerConstants {
	public static final String BOOTSTRAP_SERVER = "localhost:9092";
	public static final String AUTO_RESET_PARAM ="earliest";
	public static final String GROUP_ID ="event-news";
	public static final String SERVER_TYPE ="kafka.bootstrap.servers";
	public static final String MASTER_URL = "spark://localhost:7077";
	public static final String SPLIT_DILIMETER ="::";
	public static final String HDFS_URI = "hdfs://localhost:8020";
	public static final String DIRECTORY_HDFS = "/user/cloudera/OutputFile";
	public static String dateHour = String.valueOf(LocalDateTime.now().getDayOfYear())+String.valueOf(LocalDateTime.now().getHour());
	public static String ms = dateHour+String.valueOf(LocalDateTime.now().getSecond());

}
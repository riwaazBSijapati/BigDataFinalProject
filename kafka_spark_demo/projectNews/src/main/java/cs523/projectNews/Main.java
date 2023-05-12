package cs523.projectNews;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

public class Main {
	public static List<News> getNews(){
		CloseableHttpClient httpClient = HttpClients.createDefault();
		List<News> newsList = new ArrayList<News>();
    	try{
    		//Create a request and execute the request
    		HttpGet request = new HttpGet(NewsConstants.GET_HTTP);
    		request.addHeader("x-api-key",NewsConstants.randKey());
    		CloseableHttpResponse response = httpClient.execute(request);
    		
    		//Go through the response entity to acquire the articles list
    		HttpEntity entity = response.getEntity();
    		String retSrc = EntityUtils.toString(entity);
    		JSONObject obs = new JSONObject(retSrc);
    		JSONArray pgs = obs.getJSONArray("articles");
    		
    		//Parse through the article lists and create a new News Object using parameters from the articles
    		for (Object s:pgs) {
    			JSONObject sv = ((JSONObject) s);
    			News news = new News(
    					sv.get("_id").toString(),
    					sv.get("title").toString(),
    					sv.get("author").toString(),
    					sv.get("published_date").toString(),
    					sv.get("excerpt").toString(),
    					sv.get("topic").toString(),
    					sv.get("country").toString(),
    					sv.get("authors").toString(),
    					(boolean) sv.get("is_opinion")
    					);
    			newsList.add(news);
    		}
    	} catch (IOException e){
        	  e.printStackTrace();
        }
		return newsList;
	}
	
	public static void sendtokafka(Properties props, List<News> newsList){
    	for(News n: newsList){
    		final KafkaProducer<String,String> producer = new KafkaProducer<String,String> (props);
    		ProducerRecord<String,String> record= new ProducerRecord<>(NewsConstants.GROUP_ID,n.getId(),n.toString());
    		producer.send(record);
    	}
	}
	
    public static void main(String[] args) {
    	Properties props = new Properties();
    	props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, NewsConstants.BOOTSTRAP_SERVER);
    	props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		int i =1;
    	while(true){
			try{
				System.out.println("Kafka Job "+i+": Start Time: "+String.valueOf(LocalDateTime.now()));
				sendtokafka(props, getNews());
				TimeUnit.SECONDS.sleep(10);
				i++;
			}catch(InterruptedException ex){
				ex.printStackTrace();
			}
		}
    }
}
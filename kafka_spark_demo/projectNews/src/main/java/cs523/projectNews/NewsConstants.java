package cs523.projectNews;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class NewsConstants {
	public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	public static final String GET_HTTP ="https://api.newscatcherapi.com/v2/latest_headlines?lang=en&page_size=50";
	public static final String KEY_ONE ="nau57Q9OFyIqHsmcQNz7Bpya2wQeRBhsqS0_ZM5Q6H0";
	public static final String KEY_TWO ="hm2cE8AcumZuGJ-Hieezqqg8oYpdcYgoVR6M2JqvHm8";
	public static final String KEY_THREE ="ZgeTpjKuoGC8-WISnLg5Uj7bffwICpqHvlakDeOllac";
	public static final List<String> KEY_LIST = Arrays.asList(KEY_ONE, KEY_TWO, KEY_THREE);
	public static final String GROUP_ID ="event-news";

	public static String randKey(){
		 Random rand = new Random();
		 return (KEY_LIST.get(rand.nextInt(KEY_LIST.size())));
	}
}

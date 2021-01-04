package com.prediction.service;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.gson.JsonParser;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

@Service
public class TwitterServiceImpl {

	@Value("${twitter-api-key}")
	String consumerKey;

	@Value("${twitter-api-key-secret}")
	String consumerSecret;

	@Value("${twitter-access-token}")
	String token;

	@Value("${twitter-access-token-secret}")
	String secret;
	
	private static Logger logger = LoggerFactory.getLogger(TwitterServiceImpl.class.getName());
	private static JsonParser jsonParser = new JsonParser();

	Client client;
	
	public void shutdown() {
		logger.info("Stopping application");
		logger.info("Shutting down Client from Twitter..");
		client.stop();
		logger.info("Closing Producer..");
		// producer.close();
		logger.info("done");
	}
	
	public void run( List<String> terms, List<Location> locations) {
		logger.info("Setup");
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10);

		// create a twitter client
		client = createTwitterClient(msgQueue, terms, locations);
		
		client.connect();
		// create a kafka producer
		// KafkaProducer<String,String> producer = createKafkaProducer();
				
		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("Stopping application");
			logger.info("Shutting down Client from Twitter..");
			client.stop();
			logger.info("Closing Producer..");
			// producer.close();
			logger.info("done");
		}));
		
		// loop to sent tweets to kafka
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(3, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if(msg != null) {
				logger.info(msg);
				logger.info(" Cleaned :"+ extractCleanTweet(msg));
			}
			/*
			if(msg!= null) {
				logger.info(msg);
				producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if(e!= null) {
							logger.info("Something bad happened", e);
						}
					}
				});
			}
			*/
		}
		logger.info("End of application");		
	}
	
	public Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> terms, List<Location> locations) {
		
		
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		
		hosebirdEndpoint.trackTerms(terms);
		hosebirdEndpoint.locations(locations);
		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));     

		Client hosebirdClient = builder.build();
				
		return hosebirdClient;
		
	}
	
	public KafkaProducer<String, String> createKafkaProducer(){
		String bootstrapServer = "127.0.0.1:9092";
		
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create safe producer
		props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		// below 3 are not required as they are included in above one
		props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafak2.2 > 1.1, hence 
		// we can keep it 5, otherwise it would be 1.
		
		// high throughput user at the expense of a bit of latency and CPU usage
		props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
		
		return producer;
	}
	
	public static String extractCleanTweet(String tweetJson) {
		return jsonParser
				.parse(tweetJson)
				.getAsJsonObject()
				.get("text")
				.getAsString()
				.trim()
		        .replaceAll("http.*?[\\S]+", "")		// remove links
		        .replaceAll("@[\\S]+", "")				// remove usernames
		        .replaceAll("#", "")					// replace hashtags by just words
		        .replaceAll("[\\s]+", " ");				// correct all multiple white spaces to a single white space
				
	}
	
	
}
/*
2021-01-04 21:25:43.672  INFO 7329 --- [nio-8080-exec-1] c.prediction.service.TwitterServiceImpl  : 
{
"created_at":"Mon Jan 04 15:55:37 +0000 2021",
"id":1346123126908870657,
"id_str":"1346123126908870657",
"text":"Spectacular, Mostly-Vegan Mexican Cafe Opens On Greenpoint-Williamsburg Border https:\/\/t.co\/ncmi740s0I https:\/\/t.co\/ZPfR1pk4Cd",
"display_text_range":[0,102],
"source":"\u003ca href=\"https:\/\/mobile.twitter.com\" rel=\"nofollow\"\u003eTwitter Web App\u003c\/a\u003e",
"truncated":false,
"in_reply_to_status_id":null,
"in_reply_to_status_id_str":null,
"in_reply_to_user_id":null,
"in_reply_to_user_id_str":null,
"in_reply_to_screen_name":null,
"user":{
	"id":810424,
	"id_str":"810424",
	"name":"Gothamist",
	"screen_name":"Gothamist",
	"location":"New York, NY",
	"url":null,
	"description":"Gothamist is a website about New York City and everything that happens in it. Get Gothamist in your inbox today: https:\/\/bit.ly\/2qdn955",
	"translator_type":"none",
	"protected":false,
	"verified":true,
	"followers_count":955725,"friends_count":795,
	"listed_count":6888,
	"favourites_count":412,
	"statuses_count":125854,
	"created_at":"Sun Mar 04 16:58:18 +0000 2007",
	"utc_offset":null,
	"time_zone":null,
	"geo_enabled":true,
	"lang":null,
	"contributors_enabled":false,
	"is_translator":false,
	"profile_background_color":"E5E3E3",
	"profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png",
	"profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png",
	"profile_background_tile":false,
	"profile_link_color":"FF0000",
	"profile_sidebar_border_color":"999999",
	"profile_sidebar_fill_color":"D8D6D6",
	"profile_text_color":"191919",
	"profile_use_background_image":true,
	"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/482838063668682753\/ulq-goPG_normal.png",
	"profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/482838063668682753\/ulq-goPG_normal.png",
	"profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/810424\/1559135837",
	"default_profile":false,
	"default_profile_image":false,
	"following":null,
	"follow_request_sent":null,
	"notifications":null
},
"geo":null,
"coordinates":null,
"place":null,
"contributors":null,
"is_quote_status":false,
"quote_count":0,
"reply_count":0,
"retweet_count":0,
"favorite_count":0,
"entities":
	{
	"hashtags":[],
	"urls":[
		{
		"url":"https:\/\/t.co\/ncmi740s0I",
		"expanded_url":"https:\/\/gothamist.com\/food\/spectacular-mostly-vegan-mexican-cafe-opens-greenpoint-williamsburg-border",
		"display_url":"gothamist.com\/food\/spectacul\u2026",
		"indices":[79,102]
		}],
	"user_mentions":[],
	"symbols":[],
	"media":
		[{
		"id":1346123068255698945,
		"id_str":"1346123068255698945",
		"indices":[103,126],
		"media_url":"http:\/\/pbs.twimg.com\/media\/Eq5jzGtXAAElL5e.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/Eq5jzGtXAAElL5e.jpg","url":"https:\/\/t.co\/ZPfR1pk4Cd","display_url":"pic.twitter.com\/ZPfR1pk4Cd","expanded_url":"https:\/\/twitter.com\/Gothamist\/status\/1346123126908870657\/photo\/1","type":"photo","sizes":{"small":{"w":680,"h":453,"resize":"fit"},"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":2048,"h":1365,"resize":"fit"},"medium":{"w":1200,"h":800,"resize":"fit"}}}]},"extended_entities":{"media":[{"id":1346123068255698945,"id_str":"1346123068255698945","indices":[103,126],"media_url":"http:\/\/pbs.twimg.com\/media\/Eq5jzGtXAAElL5e.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/Eq5jzGtXAAElL5e.jpg","url":"https:\/\/t.co\/ZPfR1pk4Cd","display_url":"pic.twitter.com\/ZPfR1pk4Cd","expanded_url":"https:\/\/twitter.com\/Gothamist\/status\/1346123126908870657\/photo\/1","type":"photo","sizes":{"small":{"w":680,"h":453,"resize":"fit"},"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":2048,"h":1365,"resize":"fit"},"medium":{"w":1200,"h":800,"resize":"fit"}}},{"id":1346123102569324544,"id_str":"1346123102569324544","indices":[103,126],"media_url":"http:\/\/pbs.twimg.com\/media\/Eq5j1GiXYAARJ1v.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/Eq5j1GiXYAARJ1v.jpg","url":"https:\/\/t.co\/ZPfR1pk4Cd","display_url":"pic.twitter.com\/ZPfR1pk4Cd","expanded_url":"https:\/\/twitter.com\/Gothamist\/status\/1346123126908870657\/photo\/1","type":"photo","sizes":{"small":{"w":680,"h":453,"resize":"fit"},"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":2048,"h":1365,"resize":"fit"},"medium":{"w":1200,"h":800,"resize":"fit"}}}]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"en","timestamp_ms":"1609775737870"}


*/

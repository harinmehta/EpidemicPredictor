package com.prediction.service;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
	
	@Autowired
	SentimentService sentimentService;
	
	private static Logger logger = LoggerFactory.getLogger(TwitterServiceImpl.class.getName());
	private static JsonParser jsonParser = new JsonParser();
	public int positive, very_positive, negative, very_negative, neutral;
	private static final String HOSEBIRD_CLIENT = "Hosebird-Client-01";

	private static Client client;
	
	public void run( List<String> terms, List<Location> locations) {
		positive = very_positive = negative = very_negative = neutral = 0;
		logger.info("Setup");
		
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10);

		// create a twitter client
		client = createTwitterClient(msgQueue, terms, locations);
		client.connect();
				
		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("Stopping application");
			logger.info("Shutting down Client from Twitter..");
			client.stop();
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
				String cleanMsg = extractCleanTweet(msg);
				int sentiment = sentimentService.analyse(cleanMsg);
				logger.info(">>>>>>>Cleaned :"+ cleanMsg +"\n>>>>>>>Sentiment :"+sentiment);
				if(cleanMsg.length() > 4) {
					switch(sentiment) {
						case 0 : very_negative++;
								 break;
						case 1 : negative++;
						 		 break;
						case 2 : neutral++;
				 		 		 break;
						case 3 : positive++;
				 		 		 break;
						case 4 : very_positive++;
				 		 		 break;
					}
				}
			}
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
				  .name(HOSEBIRD_CLIENT)                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));     

		Client hosebirdClient = builder.build();
				
		return hosebirdClient;
		
	}
	
	public void shutdown() {
		logger.info("Stopping application");
		logger.info("Shutting down Client from Twitter..");
		client.stop();
		logger.info("done");
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
		        .replaceAll("[\\s]+", " ")				// correct all multiple white spaces to a single white space
		        .trim();		
	}
}
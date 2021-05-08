package com.prediction.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Lists;
import com.prediction.model.ObjectWrapper;
import com.prediction.service.TwitterServiceImpl;
import com.twitter.hbc.core.endpoint.Location;

@RestController
@RequestMapping("/")
public class BaseController {
	
	@Autowired
	TwitterServiceImpl twitterService;
	
	private static Logger logger = LoggerFactory.getLogger(BaseController.class);
	
	@PostMapping("start")
	public void fetchTweets(@RequestBody ObjectWrapper objectWrapper, BindingResult br) {
		if(br.hasErrors()) {
			logger.error("Binding Result caught errors.");
			return;
		}
		List<String> terms = objectWrapper.getTerms();
		logger.info("Terms: " + terms.toString());
		List<Location> locations = 
				Lists.newArrayList(
						new Location(
								new Location.Coordinate(objectWrapper.getSouthWestLongitude(), 
															objectWrapper.getSouthWestLatitude()), 
								new Location.Coordinate(objectWrapper.getNorthEastLongitude(), 
															objectWrapper.getNorthEastLatitude())));
		twitterService.run(terms, locations);
	}
	@PostMapping("/stop")
	public void stop() {
		logger.info("Stopping service");
		twitterService.shutdown();
	}
	
	@PostMapping("/analyse")
	public Map<Object, Object> analyse() {
		Map<Object, Object> map = new HashMap<>();
		map.put("Very Negative", twitterService.very_negative); 
		map.put("Negative", twitterService.negative);
		map.put("Neutral", twitterService.neutral);
		map.put("Positive", twitterService.very_positive);
		map.put("Very Positive", twitterService.positive);
		return map;
	}
}

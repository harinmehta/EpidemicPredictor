package com.prediction.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Lists;
import com.prediction.service.TwitterServiceImpl;
import com.twitter.hbc.core.endpoint.Location;

@RestController
@RequestMapping("/")
public class BaseController {
	
	@Autowired
	TwitterServiceImpl twitterService;
	
	@GetMapping()
	public void fetchTweets() {
		List<String> terms = Lists.newArrayList("Vegan");
		List<Location> locations = Lists.newArrayList(new Location( new Location.Coordinate(68.116667 ,8.066667), new Location.Coordinate(97.416667,37.100000)));
		twitterService.run(terms, locations);
	}
	@GetMapping("/stop")
	public void stop() {
		twitterService.shutdown();
	}
}

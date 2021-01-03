package com.prediction.controller;

import org.springframework.web.bind.annotation.*;

@RestController
public class BaseController {

	@RequestMapping("/")
	public String base() {
		return "success";
	}
	
}

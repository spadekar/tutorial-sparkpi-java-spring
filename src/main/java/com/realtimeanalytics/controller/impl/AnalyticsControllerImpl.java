package com.realtimeanalytics.controller.impl;

import com.realtimeanalytics.constant.IConstant;
import com.realtimeanalytics.controller.AnalyticsController;
import com.realtimeanalytics.service.AnalyticsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin(origins = "*")
public class AnalyticsControllerImpl implements AnalyticsController, IConstant{
	
	@Autowired
	AnalyticsService analyticsService;
	
	@CrossOrigin(origins="*")
    @RequestMapping("/analyticsinfo")
    public Object getAnalyticsinfo() {
    	
    	Object object = null;
    	
    	object = analyticsService.getAnalyticsinfo();
    	
    	return object;
    	
    }
    
	@CrossOrigin(origins="*")
	@RequestMapping("/customers")
	public Object getCustomersInfo() {
    	
    	Object object = null;
    	
    	object = analyticsService.getCustomersInfo();
    	
    	return object;
    	
    }

}

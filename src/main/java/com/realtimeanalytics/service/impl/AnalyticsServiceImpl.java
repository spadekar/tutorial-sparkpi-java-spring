package com.realtimeanalytics.service.impl;

import com.realtimeanalytics.app.data.GenericDao;
import com.realtimeanalytics.constant.IConstant;
import com.realtimeanalytics.exception.ClientException;
import com.realtimeanalytics.exception.DBException;
import com.realtimeanalytics.service.AnalyticsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class AnalyticsServiceImpl implements AnalyticsService, IConstant{
	
	@Autowired
	GenericDao genericDao;

	public Object getAnalyticsinfo() {
		
		try {
			Query query= new Query();
			List<Object> data = genericDao.getMongoTemplate().find(query, Object.class, REALTIMEANALYTICS_COLLECTION);
			
			if(data.isEmpty()) {
				throw new ClientException();
			}			
			return data;
			
		}catch(NumberFormatException e){			
			throw new NumberFormatException();
		}catch(DataAccessResourceFailureException d){			
			throw new DBException(d);
		}
		
	}

	public Object getCustomersInfo() {
		
		try {
			Query query= new Query();
			List<Object> data = genericDao.getMongoTemplate().find(query, Object.class, REALTIMEANALYTICS_DATA_COLLECTION);
			
			if(data.isEmpty()) {
				throw new ClientException();
			}			
			return data;
			
		}catch(NumberFormatException e){			
			throw new NumberFormatException();
		}catch(DataAccessResourceFailureException d){			
			throw new DBException(d);
		}
		
	}

}

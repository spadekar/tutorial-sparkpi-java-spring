package com.realtimeanalytics.app.data;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.stereotype.Component;

@Component
public class GenericDao {
	
private final MongoTemplate mongoTemplate;
	
	//private static final Logger LOGGER = LogManager.getLogger(GenericDao.class);
	
	@Autowired
	public GenericDao(@Value("${mongodb.uri}") String URI, @Value("${mongodb.database}") String database){
		//LOGGER.info("URI: "+URI);
		MongoClient client = new MongoClient(new MongoClientURI(URI));
		SimpleMongoDbFactory simpleMongoDbFactory = new SimpleMongoDbFactory(client, database);
		mongoTemplate = new MongoTemplate(simpleMongoDbFactory);		
		
	}
	
	public MongoTemplate getMongoTemplate() {
		return mongoTemplate;
	}

}

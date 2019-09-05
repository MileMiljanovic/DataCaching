package com.devoteam.kafka.kafkaproducer;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class UserResource {
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	private static final String TOPIC = "redistopic";
	private static final Integer RECORDS_COUNT = 100000;
	
	@Bean
	public void post() throws InterruptedException, JsonProcessingException {

		for(Integer i = 1; i <= RECORDS_COUNT; i++) {
			
			Model m = new Model();
			Date d = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			String strDate = sdf.format(d);
			m.setId("user:" + i);
			m.setUsername("User" + i);
			m.setPassword("Password" + i);
			m.setAge(26);
			m.setGender("male");
			m.setAttribute1("attr1:" + i);
			m.setAttribute2("attr2:" + i);
			m.setAttribute3("attr3:" + i);
			m.setAttribute4("attr4:" + i);
			m.setAttribute5("attr5:" + i);
			m.setAttribute6("attr6:" + i);
			m.setCreationTime(strDate);
			m.setOperation("INSERT");
			m.setRowId("DUMMY");
			
			ObjectMapper objectMapper = new ObjectMapper();
			String json = objectMapper.writeValueAsString(m);

			kafkaTemplate.send(TOPIC, json);
			/*System.out.println("MESSAGE SENT!: " + json);
			Thread.sleep(1000);*/
		}
		
	}

}

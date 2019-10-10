package com.devoteam.kafka.kafkaconsumer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import java.util.HashSet;


@Service
public class Listener {
	
	private Jedis jedis;
	//private JedisCluster jedisCluster;
	
	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
	
	@Value("${redis.host}")
    private String host;
	
	@Value("${redis.port}")
    private Integer port;
	
	@Value("${redis.password}")
    private String password;
	
	@Value("${redis.retry.interval.ms}")
    private Integer retryInterval;
	
	@Value("${logging.file}")
    private String loggingFile;
	
	@Value("${spring.log.level}")
    private String loggingLevel;
	
	private static Logger LOGGER = Logger.getLogger(Listener.class.getName());
	
	@PostConstruct
	public void init() throws InterruptedException, SecurityException, IOException {
		
		//initialize logger handler (output file and console)
		setLoggerHandler();  //set new handlers
		LOGGER.log(Level.INFO, "Kafka consumer initialized!");
		//initialize connection towards redis
		initializeRedis();
		
		//cluster setup, usage to be discussed
		/*HashSet<HostAndPort> connectionPoints = new HashSet<HostAndPort>();
		connectionPoints.add(new HostAndPort("10.0.200.232", 7000));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7001));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7002));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7003));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7004));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7005));

        jedisCluster = new JedisCluster(connectionPoints);*/
	}
	
	public void initializeRedis() throws InterruptedException {
		
		try {			
			jedis = new Jedis(host, port);    //try to connect to redis
			jedis.auth(password);
			LOGGER.log(Level.INFO, "Pinging Redis...");
			jedis.ping();
			kafkaListenerEndpointRegistry.start();   //enable message consumption from kafka
			LOGGER.log(Level.INFO, "Connection to redis successful!");
		}
		catch (Exception e) {	//if connecting to redis was not successful
			jedis.close();   //close session
			kafkaListenerEndpointRegistry.stop();  //stop message consumption from kafka
			LOGGER.log(Level.SEVERE, "Connection to redis failed!");
			LOGGER.log(Level.SEVERE, "CAUSE: "  + e.getCause() + "; ERROR MESSAGE - " + e.getMessage());
			Thread.sleep(retryInterval);	//attempt to reconnect every X seconds, specified in properties file
			LOGGER.log(Level.INFO, "ATTEMPTING TO RECONNECT TO REDIS");
			initializeRedis();	//try to reconnect again
		}
	}
	
	public void setLoggerHandler() throws SecurityException, IOException {
		
		//upon bean creation disable default logger handlers
		LOGGER.setUseParentHandlers(false);
		Level level = Level.parse(loggingLevel);  //get logging level specified in properties
		
		FileHandler handler = new FileHandler(loggingFile, true);	//create new file handler, specify path and append flag
		handler.setFormatter(new SimpleFormatter() {
			private static final String format = "[%1$tF %1$tT.%1$tL] [%2$-7s] %3$s %n";	//define log format

            @Override
            public synchronized String format(LogRecord lr) {
                return String.format(format,
                        new Date(lr.getMillis()),
                        lr.getLevel().getLocalizedName(),
                        lr.getMessage()
                );
            }
        });
		LOGGER.addHandler(handler);	//add a file handler to the logger
		ConsoleHandler chandler = new ConsoleHandler();	 //create a console handler and add it to the logger
		LOGGER.addHandler(chandler);
		handler.setLevel(level);	//set the logging level of the handler as specified in properties
		chandler.setLevel(level);
		LOGGER.setLevel(level);  //set the level to the one specified in properties
	}
	
	@KafkaListener(topics = "${kafka.topic}", id = "kafkalistener", groupId = "${kafka.group.id}")
	public void consume(ConsumerRecord<Integer, String> message) throws InterruptedException  {	
			
		Model m = parseJson(message);   //try to parse the incoming message
		
		if (m != null) {    //if parse was successful, try to insert the record into redis
			
			if (m.getOperation().equals("DELETE")) {
				deleteRecord(m, message.timestamp());
			}
			else if (m.getOperation().equals("INSERT") || m.getOperation().equals("UPDATE")) {
				insertRecord(m, message.timestamp());
			}
			else LOGGER.log(Level.INFO, "Non-CUD operation performed, Redis will not be called!");
		}
		
	}
	
	public Model parseJson(ConsumerRecord<Integer, String> message) {
		try {
			ObjectMapper objectMapper = new ObjectMapper();		 
			Model m = objectMapper.readValue(message.value(), Model.class);	//deserialize the JSON message we received, create model object
			LOGGER.log(Level.INFO, "Record parsed successfully. Record id: " + m.getId());
			return m;
		}
		catch (Exception e) {
			LOGGER.log(Level.SEVERE, "JSON parse failed due to invalid record!");	//if parse was unsuccessful, record is not valid, ignore it
			LOGGER.log(Level.SEVERE, "CAUSE: "  + e.getCause() + "; ERROR MESSAGE - " + e.getMessage());
			return null;
		}
	}
	
	public void insertRecord(Model m, long timestampKafka) throws InterruptedException {   //function which inserts the record into redis 
		HashMap<String, String> hmap = new HashMap<String, String>();  //hashmap is used to insert a new hash to redis
		hmap.put("id", String.valueOf(m.getId()));
		hmap.put("username", String.valueOf(m.getUsername()));
		hmap.put("password", String.valueOf(m.getPassword()));
		hmap.put("age", String.valueOf(m.getAge().toString()));
		hmap.put("gender", String.valueOf(m.getGender()));
		hmap.put("attribute1", String.valueOf(m.getAttribute1()));
		hmap.put("attribute2", String.valueOf(m.getAttribute2()));
		hmap.put("attribute3", String.valueOf(m.getAttribute3()));
		hmap.put("attribute4", String.valueOf(m.getAttribute4()));
		hmap.put("attribute5", String.valueOf(m.getAttribute5()));
		hmap.put("attribute6", String.valueOf(m.getAttribute6()));
		try {
			jedis.hmset(m.getId(), hmap);   //add a new hash to redis
			if (m.getOperation().equals("INSERT")) {
				jedis.set(m.getRowId(), m.getId()); //HELPER STRCUTURE FOR DELETE
			}
			Date writeTime = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			Date creationTime = sdf.parse(m.getCreationTime());
			long diffAbsolute = writeTime.getTime() - creationTime.getTime();
			long diffKafkaConsumer = writeTime.getTime() - timestampKafka;
			long diffKafkaProducer = timestampKafka - creationTime.getTime();
			LOGGER.log(Level.INFO, "Record successfully written to Redis! Record id: " + m.getId() + 
					". Total latency time is " + diffAbsolute + " ms." +
					" Time to process from producer to kafka: " + diffKafkaProducer + " ms." +
					" Time to process from kafka to redis: " + diffKafkaConsumer + " ms.");
		}
		catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Writing to Redis has failed!");
			LOGGER.log(Level.SEVERE, "CAUSE: "  + e.getCause() + "; ERROR MESSAGE - " + e.getMessage());
			initializeRedis();   //if we lost the connection to redis, attempt to reconnect
		}
	}
	
	public void deleteRecord(Model m, long timestampKafka) throws InterruptedException {
		try {
			String id = jedis.get(m.getRowId()); //USING HELPER STRUCTURE TO DELETE RECORD
			jedis.del(id);
			Date writeTime = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			Date creationTime = sdf.parse(m.getCreationTime());
			long diffAbsolute = writeTime.getTime() - creationTime.getTime();
			long diffKafkaConsumer = writeTime.getTime() - timestampKafka;
			long diffKafkaProducer = timestampKafka - creationTime.getTime();
			LOGGER.log(Level.INFO, "Record successfully deleted from Redis! Record id: " + m.getId() + 
					". Total latency time is " + diffAbsolute + " ms." +
					" Time to process from producer to kafka: " + diffKafkaProducer + " ms." +
					" Time to process from kafka to redis: " + diffKafkaConsumer + " ms.");
		}
		catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Deleting from Redis has failed!");
			LOGGER.log(Level.SEVERE, "CAUSE: "  + e.getCause() + "; ERROR MESSAGE - " + e.getMessage());
			initializeRedis();   //if we lost the connection to redis, attempt to reconnect
		}
	}
	
	
	@PreDestroy
	public void delete() throws IOException {    //before we destroy the bean, close the connection towards redis
		jedis.close();
	}

}

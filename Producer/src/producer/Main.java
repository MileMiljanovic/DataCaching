package producer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Main {
	
	private final static String TOPIC = "redistopic";
	private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static Integer RECORD_NUMBER = 100000;

	@SuppressWarnings("unused")
	public static void main(String[] args) throws ExecutionException, InterruptedException, JsonProcessingException {
		// TODO Auto-generated method stub
		
		final Producer<Long, String> producer = createProducer();

	    try {
	    	for (int i = 1; i <= RECORD_NUMBER; i++) {
	    		String jsonMessage = serialize(i);
	    		final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, jsonMessage);
	            RecordMetadata metadata = producer.send(record).get();
	            //System.out.println("MESSAGE SENT TO TOPIC " + TOPIC + ". Content: " + jsonMessage);
	    	}
	    }
	    
	    finally {
	        producer.flush();
	        producer.close();
	    }
		
	    //System.out.println(getIp());

	}
	
	public static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
	}
	
	public static String serialize(Integer i) throws JsonProcessingException {
		
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
		m.setRowId("DUMMY:" + i);
		
		ObjectMapper objectMapper = new ObjectMapper();
		String json = objectMapper.writeValueAsString(m);
		return json;
	}
	
	/*public static String getIp() {
		
		String ip = null;
		try {
		    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		    while (interfaces.hasMoreElements()) {
		        NetworkInterface iface = interfaces.nextElement();
		        if (iface.isLoopback() || !iface.isUp())
		            continue;

		        Enumeration<InetAddress> addresses = iface.getInetAddresses();
		        while(addresses.hasMoreElements()) {
		            InetAddress addr = addresses.nextElement();

		            if (addr instanceof Inet6Address) continue;

		            ip = addr.getHostAddress();
		            System.out.println(iface.getDisplayName() + " " + ip);
		        }
		    }
		} catch (SocketException e) {
		    throw new RuntimeException(e);
		}
		return ip;
	}*/

}

package main;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleDriver;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.dcn.DatabaseChangeEvent;
import oracle.jdbc.dcn.DatabaseChangeListener;
import oracle.jdbc.dcn.DatabaseChangeRegistration;

public class Main {
	
	static final String USERNAME = "mmiljanovic";
    static final String PASSWORD = "mile";
    static final String TOPIC = "redistopic";
	static final String BOOTSTRAP_SERVER = "localhost:9092";
    static final String URL = "jdbc:oracle:thin:@10.0.200.33:1521:xe";

    @SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		
		OracleConnection conn = connect();
        Properties prop = new Properties();
        prop.setProperty(OracleConnection.DCN_NOTIFY_ROWIDS, "true");
        DatabaseChangeRegistration dcr = conn.registerDatabaseChangeNotification(prop);
        
        final Producer<Long, String> producer = createProducer();

        try {
            dcr.addListener(new DatabaseChangeListener() {
                public void onDatabaseChangeNotification(DatabaseChangeEvent dce) {
                	if (dce.getRegId() == dcr.getRegId()) {
                		for (int i = 0; i < dce.getTableChangeDescription()[0].getRowChangeDescription().length; i++) {
                			String event = dce.getTableChangeDescription()[0].getRowChangeDescription()[i].getRowOperation().toString();
                			String rowId = dce.getTableChangeDescription()[0].getRowChangeDescription()[i].getRowid().stringValue();
                			if(event.trim().equals("INSERT") || event.trim().equals("UPDATE") || event.trim().equals("DELETE")) {
		                        try {
			                        if(event.trim().equals("DELETE")) {
			                        	deleteRow(event, rowId, producer);
			                        }
			                        else {
			                        	upsertRow(event, rowId, producer, conn);
			                        }
		                        }
		                        catch (SQLException ex) {
		                            ex.printStackTrace();
		                        }
	                		}
                			
                		}
                	}
                }
            });
            Statement stmt = conn.createStatement();
            ((OracleStatement) stmt).setDatabaseChangeRegistration(dcr);
            ResultSet rs = stmt.executeQuery("select * from USERS");
            while (rs.next()) {}
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            if (conn != null) {
                conn.unregisterDatabaseChangeNotification(dcr);
                conn.close();
                producer.flush();
    	        producer.close();
            }
            throw ex;
        }

	}
    
    public static void deleteRow(String event, String rowId, Producer<Long, String> producer) {
    	Model m = new Model();
    	Date d = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String strDate = sdf.format(d);
		m.setOperation(event);
		m.setRowId(rowId);
		m.setCreationTime(strDate);
		try {
			String jsonMessage = serialize(m);
    		final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, jsonMessage);
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("MESSAGE SENT TO TOPIC " + TOPIC + ". Content: " + jsonMessage);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    public static void upsertRow(String event, String rowId, Producer<Long, String> producer, OracleConnection conn) throws SQLException {
    	Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from USERS where ROWID='" + rowId + "'");
        while (rs.next()) {
        	String id = rs.getString("id");
        	String username = rs.getString("username");
        	String pass = rs.getString("password");
        	Integer age = rs.getInt("age");
        	String gender = rs.getString("gender");
        	String attribute1 = rs.getString("attribute1");
        	String attribute2 = rs.getString("attribute2");
        	String attribute3 = rs.getString("attribute3");
        	String attribute4 = rs.getString("attribute4");
        	String attribute5 = rs.getString("attribute5");
        	String attribute6 = rs.getString("attribute6");
        	System.out.println("CHANGED ROW: " + id + "; " + username + "; " + pass + "; " +  age + "; " +  gender + "; " + 
        			attribute1 + "; " +  attribute2 + "; " +  attribute3 + "; " + attribute4 + "; " + attribute5 + "; " + attribute6);
        	Model m = new Model();
        	Date d = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			String strDate = sdf.format(d);
			m.setId(id);
			m.setUsername(username);
			m.setPassword(pass);
			m.setAge(age);
			m.setGender(gender);
			m.setAttribute1(attribute1);
			m.setAttribute2(attribute2);
			m.setAttribute3(attribute3);
			m.setAttribute4(attribute4);
			m.setAttribute5(attribute5);
			m.setAttribute6(attribute6);
			m.setCreationTime(strDate);
			m.setOperation(event);
			m.setRowId(rowId);
			try {
    			String jsonMessage = serialize(m);
	    		final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, jsonMessage);
	            RecordMetadata metadata = producer.send(record).get();
	            System.out.println("MESSAGE SENT TO TOPIC " + TOPIC + ". Content: " + jsonMessage);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
        }
        rs.close();
        stmt.close();
    }
	
	
	public static OracleConnection connect() throws SQLException {
        OracleDriver dr = new OracleDriver();
        Properties prop = new Properties();
        prop.setProperty("user", USERNAME);
        prop.setProperty("password", PASSWORD);
        return (OracleConnection) dr.connect(URL, prop);
    }
	
	public static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
	}
	
	public static String serialize(Model m) throws JsonProcessingException {
		
		ObjectMapper objectMapper = new ObjectMapper();
		String json = objectMapper.writeValueAsString(m);
		return json;
	}

}

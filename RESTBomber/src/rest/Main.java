package rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class Main {
	
	public static final String HOST = "localhost";
	public static final String PORT = "8080";
	public static final Integer NO_RECORDS = 5000;

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		
		Integer i = 0;
		while (i <= NO_RECORDS) {
			URL url = new URL("http://" + HOST + ":" + PORT + "/readItems/" + i.toString());
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");

			if (conn.getResponseCode() != 200) {
				System.out.println("Failed : HTTP error code : "
						+ conn.getResponseCode());
				i++;
				continue;
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
				(conn.getInputStream())));

			String output;
			System.out.println("RESPONSE:");
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}
			conn.disconnect();
			i++;
		}

	}
	


}

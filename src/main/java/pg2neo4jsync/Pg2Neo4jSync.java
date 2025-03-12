package pg2neo4jsync;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import com.google.gson.Gson;
import com.google.gson.JsonObject;


public class Pg2Neo4jSync {

	public static void main(String[] args) throws Exception {
		System.out.println(Stream.of(args).collect(Collectors.joining(",")));
		Pg2Neo4jSync pg2neo = new Pg2Neo4jSync(args[0], args[1], args[2], args[3], args[4]);
//		pg2neo.disableTrustManager();
		pg2neo.listen();
	}
	
	final String pgJdbcUrl, pgUser, pgPassword;
	final String neo4jApiUrl, neo4JAuth;
	
	public Pg2Neo4jSync(String pgJdbcUrl, String pgUser, String pgPassword, String neo4jApiUrl, String neo4jAuth) {
		this.pgJdbcUrl = pgJdbcUrl;
		this.pgUser = pgUser;
		this.pgPassword = pgPassword;
		this.neo4jApiUrl = neo4jApiUrl;
		this.neo4JAuth = neo4jAuth;
	}

	public void listen() throws Exception {
		Class.forName("org.postgresql.Driver");
		Connection lConn = DriverManager.getConnection(pgJdbcUrl, pgUser, pgPassword);
		PGConnection pgCon = lConn.unwrap(PGConnection.class);
		
		Statement st = lConn.createStatement();
		st.execute("LISTEN fileadd");
		st.execute("LISTEN filedel");

		while (true) {
			PGNotification[] notifications = pgCon.getNotifications(0);

			Gson gson = new Gson();

//			try (var session = neo4jDriver.session()) {
			for (PGNotification notification : notifications) {
				String channel = notification.getName();
				String payload = notification.getParameter();

				System.out.println(payload);
				
				if (channel.startsWith("fileadd")) {
					
					JsonObject json = gson.fromJson(payload, JsonObject.class);
					JsonObject metaData = json.remove("metadata").getAsJsonObject();
					String nodeType = metaData.get("nodetype").getAsString();
					String idAttr = metaData.get("idAttr").getAsString();
					
					String linkedResourceAttrs = "";
					for (String key : json.keySet()) {
						linkedResourceAttrs += key + ": " + json.get(key).toString() + ",";
					}
					linkedResourceAttrs = linkedResourceAttrs.substring(0, linkedResourceAttrs.length() - 1);

					String fileAddCypher = "MATCH (target:"+nodeType+" {"+idAttr+": " + json.get("objectid")+ "}) "
							+ "CREATE (target)-[:HAS]->(lr:LinkedResource { " + linkedResourceAttrs + "}) "
							+ "RETURN target";
					
					sendNeo4JRequest(fileAddCypher);
				} else if(channel.startsWith("filedel")) {
					JsonObject json = gson.fromJson(payload, JsonObject.class);
					
					String fileDelCypher = "MATCH (file:LinkedResource {fid: "+ json.get("fid").getAsInt() +"}) DETACH DELETE file";
					
					sendNeo4JRequest(fileDelCypher);
				}
			}
		}
	}
	

	protected void sendNeo4JRequest(String cypher) throws Exception {
		cypher = cypher.replace('"', '\'');
		var url = new URI(neo4jApiUrl).toURL();
		var con = (HttpURLConnection) url.openConnection();
		con.setRequestMethod("POST");
		con.setRequestProperty("Content-Type", "application/json");
		con.setRequestProperty("Authorization", "Basic "+neo4JAuth);
		con.setDoOutput(true);

		String body = "{\"statement\": \"" + cypher + ";\"}";

		try (OutputStream os = con.getOutputStream()) {
			byte[] input = body.getBytes("utf-8");
			os.write(input, 0, input.length);
		}

		try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"))) {
			StringBuilder response = new StringBuilder();
			String responseLine = null;
			while ((responseLine = br.readLine()) != null) {
				response.append(responseLine.trim());
			}
			System.out.println(response.toString());
		}

	}

	// SHould only be used during development
	protected void disableTrustManager() {
		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[] { 
		    new X509TrustManager() {     
		        public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
		            return new X509Certificate[0];
		        } 
		        public void checkClientTrusted( 
		            java.security.cert.X509Certificate[] certs, String authType) {
		            } 
		        public void checkServerTrusted( 
		            java.security.cert.X509Certificate[] certs, String authType) {
		        }
		    } 
		}; 
		
        // Create all-trusting host name verifier
        HostnameVerifier allHostsValid = new HostnameVerifier() {
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        };

        // Install the all-trusting verifier
        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);

		// Install the all-trusting trust manager
		try {
		    SSLContext sc = SSLContext.getInstance("SSL"); 
		    sc.init(null, trustAllCerts, new java.security.SecureRandom()); 
		    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
		} catch (GeneralSecurityException e) {
			e.printStackTrace();
		}
	}

}



//if(channel.equals("fileadd")) {
//JsonObject json = gson.fromJson(payload, JsonObject.class);
//
////TODO: Handle target node not found
//session.executeWriteWithoutResult(tx -> {
//	var query = new Query(
//			"WITH apoc.convert.fromJsonMap('" + payload + "') AS attrs\n"
//			+ "MATCH (target {gid: "+ json.get("psiteid").getAsInt() +"})\n"
//			+ "CREATE (target)-[:HAS]->(lr:LinkedResource)\n"
//			+ "SET lr = attrs\n"
//			+ "RETURN target");
//	tx.run(query);
//});
//
//System.out.println(notification.getParameter());
//} else if(channel.equals("filedel")) {
//JsonObject json = gson.fromJson(payload, JsonObject.class);
//System.out.println(notification.getParameter());
//
//session.executeWriteWithoutResult(tx -> {
//	var query = new Query(
//			"MATCH (file:LinkedResource {fid: "+ json.get("fid").getAsInt() +"})\n"
//			+ "DETACH DELETE file;\n");
//	tx.run(query);
//});
//}
//}

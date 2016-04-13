package edu.umass.cs.csDemo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.client.ContextServiceClient;

public class SendSearchQuery
{
	public static void main(String[] args) throws IOException, JSONException, NoSuchAlgorithmException
	{
		String csIPPort = args[0];
		String[] parsed = csIPPort.split(":");
		String csIP = parsed[0];
		int csPort = Integer.parseInt(parsed[1]);
		
		ContextServiceClient<Integer> csClient 
			= new ContextServiceClient<Integer>(csIP, csPort);
		
		// context service query format
		String query = 
				"SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE geoLocationCurrentLat >= 40 "
				+ "AND geoLocationCurrentLat <= 50 AND "
				+ "geoLocationCurrentLong >= -80 AND "
				+ "geoLocationCurrentLong <= -70";
		JSONArray resultArray = new JSONArray();
		// third argument is arbitrary expiry time, not used now
		int resultSize = csClient.sendSearchQuery(query, resultArray, 300000);
		
		for(int i=0; i< resultArray.length(); i++)
		{
			System.out.println("GUID returned "+resultArray.getString(i));
		}
		System.exit(0);
	}
}
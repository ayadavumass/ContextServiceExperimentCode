package edu.umass.cs.genericExpClient;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

public class GNSTesting 
{
	public static void main(String[] args) throws Exception
	{
		String gnsHost = args[0];
		int gnsPort = Integer.parseInt(args[1]);
				
		GNSClientCommands gnsClient 
			= new GNSClientCommands();
		

		GuidEntry accountGuid = gnsClient.accountGuidCreate("gnsumass@gmail.com", "testPass");
		Thread.sleep(5000);
		System.out.println("account guid created "+accountGuid.getGuid());
		
		JSONObject attrValuePairs = new JSONObject();
		attrValuePairs.put("latitude", 32);
		attrValuePairs.put("longitude", 96);
		attrValuePairs.put("activity", "walking");
		

		for(int i=0;i<20;i++)
		{
			long start = System.currentTimeMillis();
			gnsClient.update(accountGuid.getGuid(), attrValuePairs, accountGuid);
			long end = System.currentTimeMillis();
			System.out.println("Time taken for update "+(end-start));
			Thread.sleep(100);
		}
		System.exit(0);
	}
}
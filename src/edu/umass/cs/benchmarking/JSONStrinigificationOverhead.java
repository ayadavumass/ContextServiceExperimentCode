package edu.umass.cs.benchmarking;

import java.util.Random;

import org.apache.commons.codec.binary.Hex;
import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.utils.Utils;

public class JSONStrinigificationOverhead 
{
	public static void main(String[] args) throws JSONException
	{
		int numGuids = 10000;
		Random rand = new Random(0);
		Hex hex = new Hex();
		JSONArray resultJSON = new JSONArray();
		
		long start = System.currentTimeMillis();
		for(int i=0; i<numGuids; i++)
		{
			byte[] guidBytes = new byte[20];
			rand.nextBytes(guidBytes);
			//String guidString = Utils.bytArrayToHex(guidBytes);
			byte[] charArr = hex.encode(guidBytes);
			String guidString = new String(charArr);
			
			resultJSON.put(guidString);
		}
		System.out.println("time taken "+(System.currentTimeMillis() - start)+
				" "+resultJSON.getString(0));
		
		// time to convert to json tostring
		start = System.currentTimeMillis();
		String jsonString = resultJSON.toString();
		System.out.println("JSON tostring time taken "
						+(System.currentTimeMillis() - start));
		
		
		// time from string
		start = System.currentTimeMillis();
		resultJSON = new JSONArray(jsonString);
		System.out.println("JSON fromString time taken "
				+(System.currentTimeMillis() - start));
	}
}
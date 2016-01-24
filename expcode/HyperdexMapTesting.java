package edu.umass.cs.expcode;
import java.util.HashMap;
import java.util.Map;

import org.hyperdex.client.ByteString;
import org.hyperdex.client.Client;
import org.hyperdex.client.HyperDexClientException;


public class HyperdexMapTesting
{
	// all hyperdex related constants
	public static String HYPERDEX_IP_ADDRESS;
	public static int HYPERDEX_PORT;
	
	public static void main(String[] args) throws HyperDexClientException
	{
		HYPERDEX_IP_ADDRESS = args[0];
		HYPERDEX_PORT = Integer.parseInt(args[1]);
		
		Client hyperdexClient = new Client(HYPERDEX_IP_ADDRESS, HYPERDEX_PORT);
		
		java.util.Map<String, String> activeQueryMap = new java.util.HashMap<String, String>();
		
		String rangeKey = "1";
		activeQueryMap.put("aditya", "yadav");
		
		//Map<String, Object> attrs = new HashMap<String, Object>();
		//attrs.put(ACTIVE_QUERY_MAP_NAME, activeQueryMap);
		
		//HClinetFree.put(RANGE_KEYSPACE, rangeKey, attrs);
		Map<String, Object> mapattributes = new HashMap<String, Object>();
		mapattributes.put("activeQueryMap", activeQueryMap);
		hyperdexClient.put("rangeKeyspace", rangeKey, mapattributes);
		
		
		Map<String, Object> getMap =  hyperdexClient.get("rangeKeyspace", rangeKey);
		
		@SuppressWarnings("unchecked")
		Map<ByteString, ByteString> activeQueryMapGot = 
				(Map<ByteString, ByteString>) getMap.get("activeQueryMap");
		
		
		java.util.Iterator<ByteString> activeIter =  activeQueryMapGot.keySet().iterator();
		
		while( activeIter.hasNext() )
		{
			ByteString byteStr = activeIter.next();
			String grpGUID =  byteStr.toString();
			
			ContextServiceLogger.getLogger().fine("activeIter "+grpGUID);
			
			ByteString valueStr = activeQueryMapGot.get(byteStr);
			ContextServiceLogger.getLogger().fine("Value for key "+valueStr.toString());
		}
		
		
		
		java.util.Map<Object, Object> activeQueryMap1 = new java.util.HashMap<Object, Object>();
		
		activeQueryMap1.put("aditya1", "yadav1");
		
		//Map<String, Object> attrs = new HashMap<String, Object>();
		//attrs.put(ACTIVE_QUERY_MAP_NAME, activeQueryMap);
		
		//HClinetFree.put(RANGE_KEYSPACE, rangeKey, attrs);
		Map<String, Map<Object, Object>> mapattributes1 = new HashMap<String, Map<Object, Object>>();
		mapattributes1.put("activeQueryMap", activeQueryMap1);
		hyperdexClient.map_add("rangeKeyspace", rangeKey, mapattributes1);
		
		
		getMap =  hyperdexClient.get("rangeKeyspace", rangeKey);
		

		activeQueryMapGot = (Map<ByteString, ByteString>) getMap.get("activeQueryMap");
		
		
		activeIter =  activeQueryMapGot.keySet().iterator();
		
		while( activeIter.hasNext() )
		{
			ByteString byteStr = activeIter.next();
			String grpGUID =  byteStr.toString();
			
			ContextServiceLogger.getLogger().fine("activeIter1 "+grpGUID);
			
			ByteString valueStr = activeQueryMapGot.get(byteStr);
			ContextServiceLogger.getLogger().fine("Value for key1 "+valueStr.toString());
		}
		
		
		
	}
}
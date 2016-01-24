package edu.umass.cs.expcode;
/**
 * connects to hyperdex and uses basic put get and search.
 * @author adipc
 *
 */

import java.util.*;

import org.hyperdex.client.*;
import org.hyperdex.client.Iterator;

public class HyperdexContexServiceBasicTest
{
	public static void main(String[] args) throws HyperDexClientException
	{
		Client c = new Client("127.0.0.1", 1982);
        
		/* put */
		String GUID = "guid1";
		Map<String, Object> attrs = new HashMap<String, Object>();
		
		for(int i=0;i<50;i++)
		{
			double val = ((double)i + 0.5);
			attrs.put("context"+i, val);
		}
		
		ContextServiceLogger.getLogger().fine("put: " + c.put("contextnet", GUID, attrs));
		
		/* get */
		ContextServiceLogger.getLogger().fine("got: " + c.get("contextnet", GUID));
		
		Map<String, Object> checks = new HashMap<String, Object>();
		checks.put("context0", new Range(0.0, 1.0));
		checks.put("context1", new Range(1.0, 2.0));
		
		Iterator iter = c.search("contextnet", checks);
		
		ContextServiceLogger.getLogger().fine("\n\n printing query results");
		while( iter.hasNext() )
		{
			Map<String, Object> retObj = (Map<String, Object>) iter.next();
			ContextServiceLogger.getLogger().fine("Object "+retObj.get("GUID"));
		}
    }
}
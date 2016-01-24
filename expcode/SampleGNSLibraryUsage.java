package edu.umass.cs.expcode;
import edu.umass.cs.gns.client.UniversalTcpClient;
import edu.umass.cs.gns.client.util.KeyPairUtils;


public class SampleGNSLibraryUsage
{
	public static final String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
	public static final UniversalTcpClient gnsClient 
				= new UniversalTcpClient(defaultGns.split(":")[0], Integer.parseInt(defaultGns.split(":")[1]));
	
	public static final String LOCATION_KEY 					= "LOCATION_KEY";
	public static final String IPADDRESS_KEY				= "IPADDRESS_KEY";
	
	public static void main(String[] args)
	{
		// this is the target GUID for which you want to read 
		// location and ipaddress fields
		String targetGUID = "12345";
		try
		{
			// location is in form of latitude:longitude in string format.
			String location = gnsClient.fieldRead(targetGUID, LOCATION_KEY, null);
			ContextServiceLogger.getLogger().fine("Location "+location);
			
			// ip address and port is also in ip:port string format
			String ipPort = gnsClient.fieldRead(targetGUID, IPADDRESS_KEY, null);
			ContextServiceLogger.getLogger().fine("IPAddress "+ipPort);
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
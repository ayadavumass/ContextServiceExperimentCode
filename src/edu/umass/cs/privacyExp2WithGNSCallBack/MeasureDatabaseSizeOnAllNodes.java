package edu.umass.cs.privacyExp2WithGNSCallBack;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MeasureDatabaseSizeOnAllNodes
{
	public static final String NODE_NAME_PREFIX			= "serv";
	
	public static void main( String[] args ) throws IOException, InterruptedException
	{
		int numNodes = Integer.parseInt(args[0]);
		double totalSize = 0;
		
		for( int i=0; i < numNodes; i++ )
		{
			// du -h /home/mysqlDir-serv0/ | tail -1
			String command = "ssh serv"+i+" \"/proj/MobilityFirst/ayadavDir/contextServiceScripts/mysqlSize.sh "+i+"\"";
			System.out.println("Command "+command);
			Process p = Runtime.getRuntime().exec(command);
		    p.waitFor();
		    
		    BufferedReader reader =
		         new BufferedReader( new InputStreamReader(p.getInputStream()) );
		    
		    String line = "";
		    
		    while ( (line = reader.readLine())!= null )
		    {
		    	System.out.println("serv"+i+" "+line);
		    	String[] parsed = line.split(" ");
		    	String sizeString = parsed[0].trim();
		    	
		    	if( sizeString.charAt(sizeString.length()-1) == 'M' )
		    	{
		    		String numericValString = sizeString.substring(0, sizeString.length()-1);
		    		
		    		totalSize = totalSize + Double.parseDouble(numericValString);
		    	}
		    	else
		    	{
		    		assert(false);
		    	}
		    }
		}	
		System.out.println("Total database size "+totalSize);
	}
	
}
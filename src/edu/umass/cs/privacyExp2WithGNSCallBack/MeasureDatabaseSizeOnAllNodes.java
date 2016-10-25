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
		
		// du -h /home/mysqlDir-serv0/ | tail -1
		String command = "/proj/MobilityFirst/ayadavDir/contextServiceScripts/mysqlSizeBash.sh";
		System.out.println("Command "+command);
		Process p = Runtime.getRuntime().exec(command);
	    p.waitFor();
	    
	    BufferedReader reader =
	         new BufferedReader( new InputStreamReader(p.getInputStream()) );
	    
	    String line = "";
	    
	    while ( (line = reader.readLine())!= null )
	    {
	    	System.out.println(line);
	    	String[] parsed = line.split(" ");
	    	
	    	String sizeString = parsed[0].trim();
	    	System.out.println(" parse[0] "+parsed[0] +" "+parsed[1]+" sizeString "+sizeString);
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
			
		System.out.println("Total database size "+totalSize);
	}
	
}
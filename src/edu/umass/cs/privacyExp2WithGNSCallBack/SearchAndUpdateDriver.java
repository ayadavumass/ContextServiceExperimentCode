package edu.umass.cs.privacyExp2WithGNSCallBack;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;


public class SearchAndUpdateDriver
{
	// 100 seconds, experiment runs for 100 seconds
	public static int EXPERIMENT_TIME							= 100000;
	
	// 1% loss tolerance
	public static final double INSERT_LOSS_TOLERANCE			= 0.5;
	
	// 1% loss tolerance
	public static final double UPD_LOSS_TOLERANCE				= 0.5;
	
	// 1% loss tolerance
	public static final double SEARCH_LOSS_TOLERANCE			= 0.5;
	
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME							= 100000;
	
	public static final double ATTR_MIN 						= 1.0;
	public static final double ATTR_MAX 						= 1500.0;
	
	public static final String attrPrefix						= "attr";
	
	public static double numUsers 								= -1;
	
	public static int totalDistinctGuidsInACLs					= 50;
	
	public static int aclSize									= 10;
	
	
	private static String csHost 								= "";
	private static int csPort 									= -1;
	
	public static String guidPrefix								= "UserGUID";
	
	public static ExecutorService taskES;
	
	public static int myID;
	
	public static ContextServiceClient<Integer> csClient;
	
	// per sec
	public static double initRate								= 1.0;
	public static double searchQueryRate						= 1.0; //about every 300 sec
	public static double updateRate								= 1.0; //about every 300 sec
	
	public static int numAttrs									= 1;
	
	public static int numAttrsInQuery							= 1;
	
	public static double rhoValue								= 0.5;
	
	public static boolean userInitEnable						= true;
	
	// 0 for no privacy, 1 for hyperspace privacy, 2 for subspace privacy.
	public static int transformType								= -1;
	
	public static Vector<UserEntry> usersVector;
	
	public static double predicateLength 						= 0.5;
	
	
	public static void main( String[] args ) throws Exception
	{
		numUsers 		   = Double.parseDouble(args[0]);
		csHost   		   = args[1];
		csPort   		   = Integer.parseInt(args[2]);
		myID 			   = Integer.parseInt(args[3]);
		initRate 		   = Double.parseDouble(args[4]);
		searchQueryRate    = Double.parseDouble(args[5]);
		updateRate 		   = Double.parseDouble(args[6]);
		numAttrs 		   = Integer.parseInt(args[7]);
		numAttrsInQuery    = Integer.parseInt(args[8]);
		rhoValue 		   = Double.parseDouble(args[9]);
		userInitEnable	   = Boolean.parseBoolean(args[10]);
		transformType      = Integer.parseInt(args[11]);
		predicateLength    = Double.parseDouble(args[12]);
		
		
		usersVector = new Vector<UserEntry>();
				
		System.out.println("Search and update client started ");
		guidPrefix = guidPrefix+myID;
		
		if( transformType == PrivacySchemes.HYPERSPACE_PRIVACY.ordinal() )
		{
			System.out.println("Initializing HYPERSPACE_PRIVACY");
			csClient  = new ContextServiceClient<Integer>( csHost, csPort, true,
					PrivacySchemes.HYPERSPACE_PRIVACY );
		}
		else if( transformType == PrivacySchemes.SUBSPACE_PRIVACY.ordinal() )
		{
			System.out.println("Initializing SUBSPACE_PRIVACY");
			csClient  = new ContextServiceClient<Integer>( csHost, csPort, true,
					PrivacySchemes.SUBSPACE_PRIVACY );
		}

		
		System.out.println("ContextServiceClient created");
		
		taskES = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		
		if( userInitEnable )
		{
			long start 	= System.currentTimeMillis();
			new UserInitializationACL().initializaRateControlledRequestSender();
			long end 	= System.currentTimeMillis();
			System.out.println(numUsers+" initialization complete "+(end-start));
		}
		
		BothSearchAndUpdate bothSearchAndUpdate = null;
		
		bothSearchAndUpdate = new BothSearchAndUpdate();
		new Thread(bothSearchAndUpdate).start();
		
		bothSearchAndUpdate.waitForThreadFinish();
		((BothSearchAndUpdate)bothSearchAndUpdate).printStats();
		if(rhoValue < 1.0)
		{
			System.out.println("Avg anonymized ID updated "
							+csClient.getAvgAnonymizedIDUpdated());
		}
		
		System.exit(0);
	}
	
	
	public static String getSHA1(String stringToHash)
	{
		MessageDigest md=null;
		try
		{
			md = MessageDigest.getInstance("SHA-256");
		}
		catch ( NoSuchAlgorithmException e )
		{
			e.printStackTrace();
		}
       
		md.update(stringToHash.getBytes());
		byte byteData[] = md.digest();
 
		//convert the byte to hex format method 1
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < byteData.length; i++) 
		{
			sb.append(Integer.toString
       				((byteData[i] & 0xff) + 0x100, 16).substring(1));
		}
		String returnGUID = sb.toString();
		return returnGUID.substring(0, 40);
	}
}
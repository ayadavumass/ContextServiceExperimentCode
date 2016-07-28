package edu.umass.cs.contextservicebenchmarking;

/* Copyright (c) 2016 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): aditya */

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;

/**
 * This example creates an account GUID record, performs a few reads and writes
 * to its fields, and deletes the record.
 * <p>
 * Note: This example assumes that the verification step (e.g., via email) to
 * verify an account GUID's human-readable name has been disabled on the server
 * using the -disableEmailVerification option.
 * 
 * @author aditya
 */
public class SelectCallBenchmarking
{
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME							= 100000;
	// 1% loss tolerance
	public static final double INSERT_LOSS_TOLERANCE			= 0.0;
	
	// 1% loss tolerance
	public static final double UPD_LOSS_TOLERANCE				= 0.0;
	
	// 1% loss tolerance
	public static final double SEARCH_LOSS_TOLERANCE			= 0.0;
	
//	public static final String ACCOUNT_ALIAS_PREFIX 			= "admin";
//	public static final String ACCOUNT_ALIAS_SUFFIX 			= "@gns.name";
	//public static final String ACCOUNT_ALIAS 					= "admin@gns.name";
	
	
	// dallas region in texas area, for which we have weather alerts.
	public static final double LONGITUDE_MIN 					= -98.08;
	public static final double LONGITUDE_MAX 					= -96.01;
	
	public static final double LATITUDE_MAX 					= 33.635;
	public static final double LATITUDE_MIN 					= 31.854;
	
	public static final int NUM_GUIDs							= 1000;
	
	public static final int NUM_SELECT_OPER						= 1000;
	
	public static final String GUID_PREFIX 						= "GUIDPref";
	
	public static final String Latitude_Name					= "latitude";
	public static final String Longitude_Name					= "longitude";
	
	public static final int THREAD_POOL_SIZE					= 1;
	public static Random randomGen								= new Random();
	
	// replace with your account alias
	//public static GNSClientCommands client;
	public static ContextServiceClient<Integer> csClient;
	//public static GuidEntry account_guid;
	//private static List<GuidEntry>
		
	public static ExecutorService	 taskES						= null;
	
	public static final double LEFT 							= LONGITUDE_MIN;
	public static final double RIGHT 							= LONGITUDE_MAX;
	public static final double TOP 								= LATITUDE_MAX;
	public static final double BOTTOM 							= LATITUDE_MIN;
	
	public static JSONArray UPPER_LEFT ;
	//	= new GlobalCoordinate(TOP, LEFT);
	
	public static JSONArray UPPER_RIGHT;
	//= new GlobalCoordinate(TOP, RIGHT);
	
	public static JSONArray LOWER_RIGHT;
	//= new GlobalCoordinate(BOTTOM, RIGHT);
	
	public static JSONArray LOWER_LEFT;
	//= new GlobalCoordinate(BOTTOM, LEFT);
	
	/**
	 * @param args
	 * @throws InvalidKeySpecException
	 * @throws ClientException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception
	{
		taskES =  Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		
		UPPER_LEFT = new JSONArray("["+LEFT+","+TOP+"]");
//		= new GlobalCoordinate(TOP, LEFT);
		
		UPPER_RIGHT = new JSONArray("["+RIGHT+","+TOP+"]");
		//= new GlobalCoordinate(TOP, RIGHT);
		
		LOWER_RIGHT = new JSONArray("["+RIGHT+","+BOTTOM+"]");
		//= new GlobalCoordinate(BOTTOM, RIGHT);
		
		LOWER_LEFT = new JSONArray("["+LEFT+","+BOTTOM+"]");
		//= new GlobalCoordinate(BOTTOM, LEFT);
		
		/* Create the client that connects to a default reconfigurator as
		 * specified in gigapaxos properties file. */
		//client = new GNSClientCommands();
		csClient = new ContextServiceClient<Integer>("serv0", 8000);
		System.out.println("[Client connected to context service]\n");
		
		
		//insertGUIDs();
		AbstractRequestSendingClass requestTypeObj = null;
		requestTypeObj = new InsertClass();
		
		new Thread(requestTypeObj).start();
		requestTypeObj.waitForThreadFinish();
		
		issueWholeRegionSelectQuery();
		
		perfromSelectOperations();
		//client.select
		// Delete created GUID
//		client.accountGuidRemove(account_guid);
//		System.out.println("\n// GUID delete\n"
//				+ "client.accountGuidRemove(GUID) // GUID=" + account_guid);
//		
//		// Try read the entire record
//		try
//		{
//			result = client.read(account_guid);
//		} catch (Exception e) {
//			System.out.println("\n// non-existent GUID error (expected)\n"
//					+ "client.read(GUID) // GUID= " + account_guid + "\n  "
//					+ e.getMessage());
//		}
//		
//		client.close();
//		System.out.println("\nclient.close() // test successful");
	}
	
	public static void issueWholeRegionSelectQuery()
	{
		// a test select
		try
		{
			String searchQuery = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE"
					+" latitude >= "+LATITUDE_MIN+" AND latitude <= "+LATITUDE_MAX
					+ " longitude >= "+LONGITUDE_MIN+" AND longitude <= "+LONGITUDE_MAX;
			
			JSONArray resultArray = new JSONArray();
			long expiryTime = 300000;
			csClient.sendSearchQuery(searchQuery, resultArray, expiryTime);
			
			System.out.println("Total guids returned "+resultArray.length());
			assert( resultArray.length() > 0 );
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
//	public static void insertGUIDs()
//	{
//		for( int i=0; i<NUM_GUIDs; i++ )
//		{
//			String guidAlias = GUID_PREFIX + i;
//			
//			try
//			{
//				double randLat = LATITUDE_MIN + randomGen.nextDouble()*(LATITUDE_MAX - LATITUDE_MIN);
//				double randLong = LONGITUDE_MIN + randomGen.nextDouble()*(LONGITUDE_MAX - LONGITUDE_MIN);
//				JSONObject updateJSON = new JSONObject();
//				updateJSON.put(Latitude_Name, randLat);
//				updateJSON.put(Longitude_Name, randLong);
//				
//				//JSONArray array = new JSONArray(Arrays.asList(randLong, randLat));
//				System.out.println("Creating GUID alias "+guidAlias);
//				GuidEntry guidEntry = client.guidCreate(account_guid, guidAlias);
//				client.setLocation(guidEntry, randLong, randLat);
//			}
//			catch (Exception e)
//			{
//				e.printStackTrace();
//			}
//		}
//		
//		JSONArray wholeRegion = new JSONArray();
//		wholeRegion.put(UPPER_LEFT);
//		wholeRegion.put(LOWER_RIGHT);
//		
//		// a test select
//		try
//		{
//			JSONArray resultArray = client.selectWithin
//						(GNSCommandProtocol.LOCATION_FIELD_NAME , wholeRegion);
//			System.out.println("Total guids returned "+resultArray.length());
//			assert(resultArray.length() == NUM_GUIDs);	
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//	}
	
	public static void perfromSelectOperations() throws Exception
	{
		double totalTime = 0.0;
		double totalSize = 0.0;
		
		for( int i=0; i<NUM_SELECT_OPER; i++ )
		{
			double randLat1 = LATITUDE_MIN + randomGen.nextDouble()*(LATITUDE_MAX - LATITUDE_MIN);
			double randLong1 = LONGITUDE_MIN + randomGen.nextDouble()*(LONGITUDE_MAX - LONGITUDE_MIN);
			
			double randLat2 = LATITUDE_MIN + randomGen.nextDouble()*(LATITUDE_MAX - LATITUDE_MIN);
			double randLong2 = LONGITUDE_MIN + randomGen.nextDouble()*(LONGITUDE_MAX - LONGITUDE_MIN);
			
			double minLat = (randLat1>randLat2)?randLat2:randLat1;
			double maxLat = (randLat1>randLat2)?randLat1:randLat2;
			
			double minLong = (randLong1>randLong2)?randLong2:randLong1;
			double maxLong = (randLong1>randLong2)?randLong1:randLong2;			
			
			String searchQuery = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE"
					+" latitude >= "+minLat+" AND latitude <= "+maxLat
					+ " longitude >= "+minLong+" AND longitude <= "+maxLong;
			
			JSONArray resultArray = new JSONArray();
			long expiryTime = 300000;
			
			long start = System.currentTimeMillis();
			csClient.sendSearchQuery(searchQuery, resultArray, expiryTime);
			long end = System.currentTimeMillis();
			
			System.out.println("Total guids returned "+resultArray.length());
			assert( resultArray.length() > 0 );
			
			totalTime = totalTime + (end-start);
			totalSize = totalSize + resultArray.length();
			System.out.println("perfromSelectOperations "+i+" size "+resultArray.length()+" time "+(end-start));
		}
		System.out.println("Average time "+(totalTime/NUM_SELECT_OPER)+" average size "+(totalSize/NUM_SELECT_OPER));
	}
	
	public static String getSHA1(String stringToHash)
	{
		MessageDigest md = null;
		try
		{
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e)
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
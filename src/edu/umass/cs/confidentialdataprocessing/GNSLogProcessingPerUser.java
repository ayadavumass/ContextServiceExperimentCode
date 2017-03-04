package edu.umass.cs.confidentialdataprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;



public class GNSLogProcessingPerUser
{
	public static final String GNS_LOG_DIR					= "/home/ayadav/Documents/Data/confidentialUserTraces/logs";
	
	public static final String USER_TRACE_DIR				= "/home/ayadav/Documents/Data/confidentialUserTraces/individualUserGNSLogs";
	
	public static final String MERGED_USER_TRACE_DIR		= "/home/ayadav/Documents/Data/confidentialUserTraces/individualMergedGNSLogs";
	
	public static final String DUP_REM_USER_DIR				= "/home/ayadav/Documents/Data/confidentialUserTraces/removeDupGNSLogs";
	
	public static final String GEO_LOC_TIME_KEY				= "geoLocationCurrentTimestamp";
	
	public static final String USERNAME_KEY					= "username";
	
	
	public static final String GUID_KEY						= "guid";
	public static final String FIELD_KEY					= "field";
	public static final String VALUE_KEY					= "value";
	
	
	
	public static String[] filelist             			= {"server0.log.0", "server0.log.2", 
																	"server0.log.4", "server0.log.6", "server0.log.7"};
	
	//public static String[] filelist             			= {"server0.log.0"};
	
	
	private static HashMap<String, Integer> usernameMap		= new HashMap<String, Integer>();
	
	
	private static void computeDistinctGUIDs()
	{
		int numGUIDs = 0;
		for(int i=0; i<filelist.length; i++)
		{
			String filename   = filelist[i];
			
			BufferedReader br = null;
			
			try
			{
				br = new BufferedReader(new FileReader(
								GNS_LOG_DIR+"/"+filename));
				
				String sCurrentLine;
				while( (sCurrentLine = br.readLine()) != null )
				{
					if( sCurrentLine.contains("Field update:") )
					{
						int index = sCurrentLine.indexOf('{');
						String jsonString = sCurrentLine.substring(index, sCurrentLine.length());
						
						try
						{
							JSONObject jsonObject = new JSONObject(jsonString);
							
							if( jsonObject.getString("field").equals(USERNAME_KEY) )
							{
								String username = jsonObject.getString("value");
								
								if( !usernameMap.containsKey(username) )
								{
									usernameMap.put(username, numGUIDs);
									numGUIDs++;
								}
							}
							
						}
						catch (JSONException e)
						{
							e.printStackTrace();
						}
					}
				}
			}
			catch(IOException ioex)
			{
				ioex.printStackTrace();
			}
			finally
			{
				if(br != null)
				{
					try
					{
						br.close();
					}
					catch (IOException e) 
					{
						e.printStackTrace();
					}
				}
			}
		}	
		System.out.println("Number of distinct users "
										+usernameMap.size());
	}
	
	
	private static void writeIndividualUserTraces()
	{
		for( int i=0; i<filelist.length; i++ )
		{
			String filename   = filelist[i];
			
			BufferedReader br = null;
			
			try
			{
				br = new BufferedReader(new FileReader(
								GNS_LOG_DIR+"/"+filename));
				
				String sCurrentLine;
				while( (sCurrentLine = br.readLine()) != null )
				{
					if( sCurrentLine.contains("Field update:") )
					{
						int index = sCurrentLine.indexOf('{');
						String jsonString = sCurrentLine.substring(index, sCurrentLine.length());
						
						try
						{
							JSONObject jsonObject = new JSONObject(jsonString);
							String guid = jsonObject.getString(GUID_KEY);
							int guidnum = usernameMap.get(guid);
							
							BufferedWriter bw = null;
							try
							{
								String writerfile = "GNSLogUserNum"+guidnum;
								bw = new BufferedWriter(
											new FileWriter(USER_TRACE_DIR
														+"/"+writerfile, true) );
								
								bw.write(sCurrentLine+"\n");
							}
							catch(IOException ioex)
							{
								ioex.printStackTrace();
							}
							finally
							{
								if(bw != null)
								{
									bw.close();
								}
							}
						}
						catch (JSONException e)
						{
							e.printStackTrace();
						}
					}
				}
			}
			catch(IOException ioex)
			{
				ioex.printStackTrace();
			}
			finally
			{
				if(br != null)
				{
					try
					{
						br.close();
					}
					catch (IOException e) 
					{
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	
	private static void computeMergedUserTraces()
	{
		File folder = new File(USER_TRACE_DIR);
		File[] listOfFiles = folder.listFiles();
		
		for (int i = 0; i < listOfFiles.length; i++)
		{
			if ( listOfFiles[i].isFile() )
			{
				String filename = listOfFiles[i].getName();

				if( filename.startsWith("GNSLogUserNum") )
				{
					mergeUpdatesInAUser(filename);
				}
			}
			else if (listOfFiles[i].isDirectory())
			{
				//assert(false);
				System.out.println("Directory " + listOfFiles[i].getName());
			}
		}
	}
	
	
	private static void mergeUpdatesInAUser(String filename)
	{
		BufferedReader br = null;
		BufferedWriter bw = null;
		try
		{
			br = new BufferedReader(new FileReader(
					USER_TRACE_DIR+"/"+filename));
			
			bw = new BufferedWriter(new FileWriter(
						MERGED_USER_TRACE_DIR+"/"+filename));
			
			String sCurrentLine;
			
			JSONObject currJSON = null;
			String lastDateTime = "";
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				if( sCurrentLine.contains("Field update:") )
				{
					String[] parsed = sCurrentLine.split(" ");
					String date = parsed[0];
					String time = parsed[1];
					
					String[] parsed2 = time.split(":");
					// we merging records per minute
					String hourMin = parsed2[0]+":"+parsed2[1];
					
					String dateTime = date+" "+hourMin;
					
					int index = sCurrentLine.indexOf('{');
					String jsonString = sCurrentLine.substring(index, sCurrentLine.length());

					try
					{
						JSONObject jsonObject = new JSONObject(jsonString);
						
						if( currJSON == null)
						{
							currJSON = new JSONObject();
							String fieldStr = jsonObject.getString(FIELD_KEY);
							String guidStr  = jsonObject.getString(GUID_KEY);
							String valueStr = jsonObject.getString(VALUE_KEY);
							
							currJSON.put(GUID_KEY, guidStr);
							currJSON.put(fieldStr, valueStr);
							
							lastDateTime = dateTime;
						}
						else
						{
							if( !lastDateTime.equals(dateTime) )
							{
								bw.write(currJSON.toString()+"\n");
								currJSON = new JSONObject();
							}
							
							lastDateTime = dateTime;
							String fieldStr = jsonObject.getString(FIELD_KEY);
							String guidStr  = jsonObject.getString(GUID_KEY);
							String valueStr = jsonObject.getString(VALUE_KEY);
							
							currJSON.put(GUID_KEY, guidStr);
							currJSON.put(fieldStr, valueStr);
						}
					}
					catch(JSONException jsonEx)
					{
						jsonEx.printStackTrace();
					}
				}
			}
			
			// last entry
			if(currJSON != null)
				bw.write(currJSON.toString()+"\n");
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		finally
		{
			if(br != null)
			{
				try
				{
					br.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
			
			if(bw != null)
			{
				try
				{
					bw.close();
				}
				catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	
	private static void removeDupAndTimeSort() throws NoSuchAlgorithmException
	{
		File folder = new File(MERGED_USER_TRACE_DIR);
		File[] listOfFiles = folder.listFiles();
		
		for (int i = 0; i < listOfFiles.length; i++)
		{
			if ( listOfFiles[i].isFile() )
			{
				String filename = listOfFiles[i].getName();

				if( filename.startsWith("GNSLogUserNum") )
				{
					removeDupAndTimeSort(filename);
				}
			}
			else if (listOfFiles[i].isDirectory())
			{
				//assert(false);
				System.out.println("Directory " + listOfFiles[i].getName());
			}
		}
	}
	
	
	private static void removeDupAndTimeSort(String filename) throws NoSuchAlgorithmException
	{
		HashMap<String, String> dupRem = new HashMap<String, String>();
		BufferedReader br = null;
		BufferedWriter bw = null;
		
		try
		{
			br = new BufferedReader(new FileReader(
					MERGED_USER_TRACE_DIR+"/"+filename));
			
			String sCurrentLine;
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				dupRem.put(stringHash(sCurrentLine), sCurrentLine);
			}
			
			List<JSONStore> list = new LinkedList<JSONStore>();
			
			Iterator<String> hashIter = dupRem.keySet().iterator();
			
			while(hashIter.hasNext())
			{
				String hash = hashIter.next();
				
				try 
				{
					JSONObject json = new JSONObject(dupRem.get(hash));
					
					if(json.has(GEO_LOC_TIME_KEY))
					{
						JSONStore jsonst = new JSONStore();
						jsonst.jsonObject = json;
						jsonst.unixtime = (long)Double.parseDouble
										(json.getString(GEO_LOC_TIME_KEY));
						
						list.add(jsonst);
					}
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
			}
			
			list.sort(new JSONStore());
			
			if(list.size() > 0)
			{
				bw = new BufferedWriter(new FileWriter(
						DUP_REM_USER_DIR+"/"+filename));
				for(int i=0; i<list.size(); i++)
				{
					bw.write(list.get(i).jsonObject.toString()+"\n");
				}
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		finally
		{
			if(br != null)
			{
				try
				{
					br.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
			
			if(bw != null)
			{
				try
				{
					bw.close();
				}
				catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public static class JSONStore implements Comparator<JSONStore>
	{
		long unixtime;
		JSONObject jsonObject;
		
		@Override
		public int compare(JSONStore o1, JSONStore o2) {
			if(o1.unixtime < o2.unixtime)
				return -1;
			else
				return 1;
		}
	}
	
	private static String stringHash(String message) throws NoSuchAlgorithmException, UnsupportedEncodingException
	{
		MessageDigest digest = MessageDigest.getInstance("SHA-1");
		digest.update(message.getBytes("utf8"));
		byte[] digestBytes = digest.digest();
		String digestStr = javax.xml.bind.DatatypeConverter.printHexBinary(digestBytes);
		return digestStr;
	}
	
	
	
	
	public static void main(String[] args) throws NoSuchAlgorithmException
	{
		computeDistinctGUIDs();
		//writeIndividualUserTraces();
		//computeMergedUserTraces();
		//removeDupAndTimeSort();
	}
}
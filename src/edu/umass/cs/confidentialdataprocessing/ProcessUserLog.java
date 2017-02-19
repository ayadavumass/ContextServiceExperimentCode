package edu.umass.cs.confidentialdataprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ProcessUserLog
{
	public static final String CONF_USER_JSON_FILE			
						= "/home/ayadav/Documents/Data/confidentialUserTraces/confidentialJSON.txt";
	
	// contains traces from the original trace, just separated for each user.
	public static final String USER_TRAJECTORY_DIR 
						= "/home/ayadav/Documents/Data/confidentialUserTraces/inidividualUserTraces";
	
	
	// time sorts and removes duplicate for each user trace.
	public static final String PROCESSED_TRAJ_DIR 
						= "/home/ayadav/Documents/Data/confidentialUserTraces/processedIndividualTracesDupRem";
	
	
	public static final String GEO_LOC_KEY = "geoLocationCurrent";
	public static final String COORD_KEY   = "coordinates";
	public static final String TIMESTAMP   = "geoLocationCurrentTimestamp";
	public static final String UNAME_KEY   = "username";
	
			
	public static final String GUID_KEY 		= "guid";
	
	private static HashMap<String, Integer> guidMap;
	private static HashMap<String, Integer> usernameMap;
	
	private static void computeUniqueGUIDs()
	{
		BufferedReader readfile = null;
		
		guidMap = new HashMap<String, Integer>();
		usernameMap = new HashMap<String, Integer>();
		int usernum = 0;
		try
		{
			String sCurrentLine;
			readfile = new BufferedReader(new FileReader(CONF_USER_JSON_FILE));
			
			while ((sCurrentLine = readfile.readLine()) != null) 
			{
				try 
				{
					JSONObject jsoObject = new JSONObject(sCurrentLine);
					String guidString = jsoObject.getString(GUID_KEY);
					String usernameString = jsoObject.getString(UNAME_KEY);
					
					
					if(!guidMap.containsKey(guidString))
					{
						usernum++;
						guidMap.put(guidString, usernum);
						usernameMap.put(usernameString, 1);
					}	
				}
				catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
		} catch (IOException e) 
		{
			e.printStackTrace();
		} finally 
		{
			try
			{
				if (readfile != null)
					readfile.close();				
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}	
		System.out.println("guidMap "+guidMap.size()+" username map size "+usernameMap.size());
	}
	
	
	private static void writeUserJSONsInDir()
	{
		BufferedReader readfile = null;
		try
		{
			String sCurrentLine;
			readfile = new BufferedReader(new FileReader(CONF_USER_JSON_FILE));
			
			while ((sCurrentLine = readfile.readLine()) != null) 
			{
				try
				{
					JSONObject jsoObject = new JSONObject(sCurrentLine);
					String guidString = jsoObject.getString(GUID_KEY);
					int usernum = guidMap.get(guidString);
					
					//System.out.println("UserGUID "+guidString+" usernum "+usernum);
					
					String filename = USER_TRAJECTORY_DIR+"/"+"TraceUser"+usernum+".txt";
					
					
					BufferedWriter bw = new BufferedWriter(new FileWriter(filename, true));
					
					bw.write(sCurrentLine+"\n");
					bw.close();
					
				}
				catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		finally 
		{
			try
			{
				if (readfile != null)
					readfile.close();				
			} catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
	}
	
	
	private static void removingDuplicateAndTimeSorting()
	{
		Iterator<String> guidIter = guidMap.keySet().iterator();
		
		while(guidIter.hasNext())
		{
			String guidString = guidIter.next();
			int usernum = guidMap.get(guidString);
			
			String filename = USER_TRAJECTORY_DIR+"/"+"TraceUser"+usernum+".txt";
			
			
			// PROCESSED_TRAJ_DIR
			BufferedReader readfile = null;
			try
			{
				String sCurrentLine;
				readfile = new BufferedReader(new FileReader(filename));
				
				HashMap<String, JSONObject> dupRemoval 
									= new HashMap<String, JSONObject>();
				
				while ((sCurrentLine = readfile.readLine()) != null) 
				{
					try
					{
						JSONObject jsoObject = new JSONObject(sCurrentLine);
						JSONObject geoLocJSON = jsoObject.getJSONObject(GEO_LOC_KEY);
						JSONArray coordArray = geoLocJSON.getJSONArray(COORD_KEY);
						double longitude = coordArray.getDouble(0);
						double latitude = coordArray.getDouble(1);
						double timestamp = Double.parseDouble
												(jsoObject.getString(TIMESTAMP));
						
						String key = latitude+":"+longitude+":"+timestamp;
						dupRemoval.put(key, jsoObject);
					}
					catch (JSONException e)
					{
						e.printStackTrace();
					}
				}
				
				List<SortClass> toSortList = new LinkedList<SortClass>();
				
				Iterator<String> keyIter = dupRemoval.keySet().iterator();
				
				while(keyIter.hasNext())
				{
					String key = keyIter.next();
					
					String[] parsed = key.split(":");
					double timestamp = Double.parseDouble(parsed[2]);
					SortClass sortclass = new SortClass();
					sortclass.timestamp = timestamp;
					sortclass.jsonobject = dupRemoval.get(key);
					
					toSortList.add(sortclass);
				}
				toSortList.sort(new SortClass());
				
				
				
				String pfilename = PROCESSED_TRAJ_DIR+"/"+"TraceUser"+usernum+".txt";
				BufferedWriter bwriter = new BufferedWriter(new FileWriter(pfilename));
				
				for(int i=0; i<toSortList.size(); i++)
				{
					SortClass sortclass = toSortList.get(i);
					bwriter.write(sortclass.jsonobject.toString()+"\n");
				}
				bwriter.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			finally
			{
				try
				{
					if (readfile != null)
						readfile.close();				
				} catch (IOException ex) 
				{
					ex.printStackTrace();
				}
			}
			
		}
	}
	
	
	private static void removingDuplicateAndTimeSortingMethod2()
	{
		Iterator<String> guidIter = guidMap.keySet().iterator();
		
		while(guidIter.hasNext())
		{
			String guidString = guidIter.next();
			int usernum = guidMap.get(guidString);
			
			String filename = USER_TRAJECTORY_DIR+"/"+"TraceUser"+usernum+".txt";
			
			
			// PROCESSED_TRAJ_DIR
			BufferedReader readfile = null;
			String pfilename = PROCESSED_TRAJ_DIR+"/"+"TraceUser"+usernum+".txt";
			BufferedWriter bwriter = null;
			
			try
			{
				String lastLine = "";
				String sCurrentLine;
				readfile = new BufferedReader(new FileReader(filename));
				bwriter = new BufferedWriter(new FileWriter(pfilename));
				
				while( (sCurrentLine = readfile.readLine()) != null )
				{
					if( !lastLine.equals(sCurrentLine) )
					{
						lastLine = sCurrentLine;
						bwriter.write(sCurrentLine+"\n");
					}
					else
					{
						// skip the line
					}
				}
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			finally
			{
				try
				{
					if (readfile != null)
						readfile.close();
				} catch (IOException ex) 
				{
					ex.printStackTrace();
				}
				
				try
				{
					if (bwriter != null)
						bwriter.close();
				} catch (IOException ex) 
				{
					ex.printStackTrace();
				}
			}
			
		}
	}
	
	
	private static class SortClass implements Comparator<SortClass>
	{
		double timestamp ;
		JSONObject jsonobject;
		
		@Override
		public int compare(SortClass o1, SortClass o2) 
		{
			if(o1.timestamp < o2.timestamp)
				return -1;
			else
				return 1;
		}
	}
	
	
	public static void main(String[] args)
	{
		computeUniqueGUIDs();
		
		//writeUserJSONsInDir();
		
		//removingDuplicateAndTimeSortingMethod2();
	}
}
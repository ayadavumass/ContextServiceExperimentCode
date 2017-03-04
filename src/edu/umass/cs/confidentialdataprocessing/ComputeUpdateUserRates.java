package edu.umass.cs.confidentialdataprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

public class ComputeUpdateUserRates
{
	public static final String USER_TRACE_DIRECTORY 
												= "/home/ayadav/Documents/Data/confidentialUserTraces/processedIndividualTracesDupRem";
	
	//"geoLocationCurrentTimestamp"
	public static final String GEOLOC_TIME		= "geoLocationCurrentTimestamp";
	
	public static final int NUM_EVENT_THRESHOLD	= 10;
	
	public static List<UpdateRateStorage> updateStorageList;
	
	private static void computeUpdateRateOfAUser(String filename)
	{
		BufferedReader br = null;
		long numEvents =0;
		long startTimestamp = -1, endTimestamp = -1;
		try
		{
			br = new BufferedReader(new FileReader(USER_TRACE_DIRECTORY+"/"+filename));
			String sCurrentLine;
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				try
				{
					JSONObject jsonObject = new JSONObject(sCurrentLine);
					double timestamp = Double.parseDouble(jsonObject.getString(GEOLOC_TIME));
					
					if(startTimestamp == -1)
					{
						startTimestamp = (long)timestamp;
					}
					endTimestamp = (long)timestamp;
					
					
					numEvents++;
				}
				catch (JSONException e)
				{
					e.printStackTrace();
				}
			}
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
		
		double avgDays = ((endTimestamp - startTimestamp)*1.0)/(60.0*60.0*24.0); 
		if((numEvents >= NUM_EVENT_THRESHOLD) && (avgDays >= 1) )
		{
			double updateRate = (numEvents*60.0*60.0*24.0)/(endTimestamp-startTimestamp);
			UpdateRateStorage upd = new UpdateRateStorage();
			upd.filename = filename;
			upd.updateRate = updateRate;
			upd.mintimestamp = startTimestamp;
			upd.maxtimestamp = endTimestamp;
			updateStorageList.add(upd);
		}
	}
	
	
	private static void computeAverageUpdateRates()
	{
		File folder = new File(USER_TRACE_DIRECTORY);
		File[] listOfFiles = folder.listFiles();
		
		for (int i = 0; i < listOfFiles.length; i++)
		{
			if ( listOfFiles[i].isFile() )
			{
				String filename = listOfFiles[i].getName();
				
				if( filename.startsWith("TraceUser") )
				{
					computeUpdateRateOfAUser(filename);
				}
				
				//filenameList.add(listOfFiles[i].getName());
				//System.out.println("File " + listOfFiles[i].getName());
			}
			else if (listOfFiles[i].isDirectory())
			{
				//assert(false);
				System.out.println("Directory " + listOfFiles[i].getName());
		    }
		}
	}
	
	public static class UpdateRateStorage implements Comparator<UpdateRateStorage>
	{
		String filename;
		double updateRate;
		long mintimestamp;
		long maxtimestamp;
		
		@Override
		public int compare(UpdateRateStorage o1, UpdateRateStorage o2) 
		{
			if(o1.updateRate < o2.updateRate)
			{
				return -1;
			}
			else
			{
				return 1;
			}
		}
	}
	
	public static void main(String[] args)
	{
		updateStorageList = new LinkedList<UpdateRateStorage>();
		computeAverageUpdateRates();
		updateStorageList.sort(new UpdateRateStorage());
		
		System.out.println("Number of useful users "+updateStorageList.size());
		
		double perc = 0.0;
		for(int i=0; i<updateStorageList.size(); i++)
		{
			perc = ((i+1)*1.0)/updateStorageList.size();
			UpdateRateStorage upd = updateStorageList.get(i);
			Date startDate = new Date(upd.mintimestamp*1000);
			Date endDate = new Date(upd.maxtimestamp*1000);
			
			double avgDays = ((upd.maxtimestamp - upd.mintimestamp)*1.0)/(60.0*60.0*24.0);
			
			System.out.println(upd.filename+","+(perc)+","+upd.updateRate+","+startDate.toString()
						+","+endDate.toString()+","+avgDays);
		}
	}
}
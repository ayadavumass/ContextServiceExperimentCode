package edu.umass.cs.confidentialdataprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

public class ProcessLogsforStats 
{
	public static final String PROCESSED_LOGS_DIR			
		= "/home/ayadav/Documents/Data/confidentialUserTraces/processedIndividualTracesDupRem";
	
	public static final String GEO_LOC_KEY = "geoLocationCurrent";
	public static final String COORD_KEY   = "coordinates";
	public static final String TIMESTAMP   = "geoLocationCurrentTimestamp";
	public static final String UNAME_KEY   = "username";
	
	
	private static void computeUpdateAvg()
	{
		File folder = new File(PROCESSED_LOGS_DIR);
		File[] listOfFiles = folder.listFiles();
		
		for (int i = 0; i < listOfFiles.length; i++) 
		{
			if ( listOfFiles[i].isFile() )
			{
				//System.out.println("File " + listOfFiles[i].getName());
				String filename = listOfFiles[i].getName();
				
				processAFile(filename);
			}
			else if (listOfFiles[i].isDirectory())
			{
				System.out.println("Directory " + listOfFiles[i].getName());
			}
		}
	}
	
	
	private static void processAFile(String filename)
	{
		BufferedReader readfile = null;
		
		try
		{
			String sCurrentLine;
			readfile = new BufferedReader(new FileReader
								(PROCESSED_LOGS_DIR+"/"+filename));
			
			HashMap<String, DateUpdateNum> perDayMap = new HashMap<String, DateUpdateNum>();
			
			while ((sCurrentLine = readfile.readLine()) != null)
			{
				try
				{
					JSONObject jsoObject = new JSONObject(sCurrentLine);
					double unixts = Double.parseDouble(jsoObject.getString(TIMESTAMP));
					
					Date date = new Date((long)(unixts*1000.0));
					String dateString = date.getDate()+"-"+(date.getMonth()+1)
															+"-"+(date.getYear()+1900);
					
					
					if( perDayMap.containsKey(dateString) )
					{
						DateUpdateNum obj = perDayMap.get(dateString);
						obj.numupdates++;
					}
					else
					{
						DateUpdateNum obj = new DateUpdateNum();
						obj.unixtimestamp = (long)unixts;
						obj.numupdates = 1;
						obj.dateStr = dateString;
						perDayMap.put(dateString, obj);
					}
				}
				catch (JSONException e)
				{
					e.printStackTrace();
				}
			}
			
			List<DateUpdateNum> sortList = new LinkedList<DateUpdateNum>();
			
			System.out.println("\n\nfilename "+filename+"\n\n");
			
			Iterator<String> dateIter = perDayMap.keySet().iterator();
			
			while(dateIter.hasNext())
			{
				String dateString = dateIter.next();
				sortList.add(perDayMap.get(dateString));
				//System.out.println("dateString="+dateString+" "+perDayMap.get(dateString));
			}
			sortList.sort(new DateUpdateNum());
			
			
			for(int i=0; i<sortList.size(); i++)
			{
				DateUpdateNum obj = sortList.get(i);
				System.out.println("dateString="+obj.dateStr+" "
										+obj.numupdates);
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
	}
	
	private static class DateUpdateNum implements Comparator<DateUpdateNum>
	{
		long unixtimestamp;
		int numupdates;
		String dateStr;
		
		@Override
		public int compare(DateUpdateNum o1, DateUpdateNum o2) 
		{
			if(o1.unixtimestamp < o2.unixtimestamp)
				return -1;
			else
				return 1;
		}
	}
	
	public static void main(String[] args)
	{
		computeUpdateAvg();
	}
}
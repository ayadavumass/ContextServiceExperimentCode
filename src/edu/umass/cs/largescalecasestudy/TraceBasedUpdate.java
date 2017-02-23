package edu.umass.cs.largescalecasestudy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GeodeticCalculator;
import edu.umass.cs.acs.geodesy.GlobalCoordinate;

public class TraceBasedUpdate extends 
					AbstractRequestSendingClass implements Runnable
{	
	private long sumResultSize					= 0;
	
	private long sumSearchLatency				= 0;
	private long sumUpdateLatency				= 0;
	
	private long numSearchesRecvd				= 0;
	private long numUpdatesRecvd				= 0;
	
	// we don't want to issue new search queries for the trigger exp.
	// so that the number of search queries in the experiment remains same.
	// so when number of search queries reaches threshold then we reset it to 
	// the beginning.
	//private long numberSearchesSent			= 0;
	
	private final Object logReadLock			= new Object();
	
	private HashMap<String, List<JSONObject>> currentEventFileMap;
	
	
	public TraceBasedUpdate()
	{
		super( LargeNumUsers.UPD_LOSS_TOLERANCE );
		currentEventFileMap = new HashMap<String, List<JSONObject>>();
	}
	
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			rateControlledRequestSender();
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	public void rateControlledRequestSender() throws Exception
	{
		TraceReadingClass traceReadingClass = new TraceReadingClass();
		new Thread(traceReadingClass).start();
		
		while( LargeNumUsers.currRealUnixTime < LargeNumUsers.END_UNIX_TIME )
		{
			synchronized(logReadLock)
			{
				while(currentEventFileMap.size() == 0)
				{
					logReadLock.wait();
				}
				
				processEvents();
				
				currentEventFileMap.clear();
				logReadLock.notify();
			}
		}
	}
	
	
	private void processEvents()
	{
		while( currentEventFileMap.size() > 0 )
		{
			Iterator<String> fileNameIter = currentEventFileMap.keySet().iterator();
			
			// remove entries that have zero remaining events in the list.
			List<String> removedFileNameList = new LinkedList<String>();
			
			while( fileNameIter.hasNext() )
			{
				try
				{
					String filename = fileNameIter.next();
					
					List<JSONObject> jsonList = currentEventFileMap.get(filename);
					
					assert(jsonList.size() > 0);
					
					JSONObject eventJSON = jsonList.remove(0);
					
					assert(eventJSON != null);
					
					if( jsonList.size() == 0 )
					{
						removedFileNameList.add(filename);
					}
					
					JSONObject geoLocJSON = eventJSON.getJSONObject(LargeNumUsers.GEO_LOC_KEY);
					JSONArray coordArray = geoLocJSON.getJSONArray(LargeNumUsers.COORD_KEY);
					
					double logLat  = coordArray.getDouble(1);
					double logLong = coordArray.getDouble(0);
					
					GlobalCoordinate logCoord 
									= new GlobalCoordinate(logLat, logLong);
					
					BufferedReader br = null;
					
					try
					{
						br = new BufferedReader(new FileReader(LargeNumUsers.USER_INFO_FILE_NAME));
						
						String sCurrentLine;
						
						while( (sCurrentLine = br.readLine()) != null )
						{
							String[] parsed = sCurrentLine.split(",");
							
							String guid = parsed[0];
							String guidfilename = parsed[1];
							double distanceInMeters = Double.parseDouble(parsed[2]);
							double startAngle = Double.parseDouble(parsed[3]);
							
							if( guidfilename.equals(filename) )
							{								
								GlobalCoordinate transformedCoord 
										= GeodeticCalculator.calculateEndingGlobalCoordinates
															(logCoord, startAngle, distanceInMeters);
								
								long reqNum = -1;
								synchronized(waitLock)
								{
									numSent++;
									reqNum = numSent;
								}
								
								JSONObject attrValJSON = new JSONObject();
								
								attrValJSON.put(LargeNumUsers.LATITUDE_KEY, 
															transformedCoord.getLatitude());
								attrValJSON.put(LargeNumUsers.LONGITUDE_KEY, 
															transformedCoord.getLongitude());
								
								
								ExperimentUpdateReply updateRep 
										= new ExperimentUpdateReply(reqNum, guid);
								
								LargeNumUsers.csClient.sendUpdateWithCallBack
													(guid, null, attrValJSON, -1, 
															updateRep, this.getCallBack());
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
							try {
								br.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
					
				}
				catch(JSONException jsonex)
				{
					jsonex.printStackTrace();
				}
			}
			
			
			for(int i=0; i<removedFileNameList.size(); i++)
			{
				String filename = removedFileNameList.get(i);
				currentEventFileMap.remove(filename);
			}
		}
		assert(currentEventFileMap.size() == 0);
	}
	
	
	public double getAverageUpdateLatency()
	{
		return (this.numUpdatesRecvd>0)?sumUpdateLatency/this.numUpdatesRecvd:0;
	}
	
	public double getAverageSearchLatency()
	{
		return (this.numSearchesRecvd>0)?sumSearchLatency/this.numSearchesRecvd:0;
	}
	
	public long getNumUpdatesRecvd()
	{	
		return this.numUpdatesRecvd;
	}
	
	public long getNumSearchesRecvd()
	{
		return this.numSearchesRecvd;
	}
	
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			this.numUpdatesRecvd++;
			
			if(numRecvd%1000 == 0)
			{
				System.out.println("AverageUpdateLatency "+getAverageUpdateLatency()
				                   +" NumUpdatesRecvd "+getNumUpdatesRecvd());
			}
			
			//System.out.println("AverageUpdateLatency "+getAverageUpdateLatency()
            //			+" NumUpdatesRecvd "+getNumUpdatesRecvd());
			
			//if(currNumReplyRecvd == currNumReqSent)
			this.sumUpdateLatency = this.sumUpdateLatency + timeTaken;
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			this.numSearchesRecvd++;
			sumResultSize = sumResultSize + resultSize;
			
			this.sumSearchLatency = this.sumSearchLatency + timeTaken;
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
	
	
	private class TraceReadingClass implements Runnable
	{	
		@Override
		public void run()
		{
			while( LargeNumUsers.currRealUnixTime <  LargeNumUsers.END_UNIX_TIME )
			{
				synchronized(logReadLock)
				{
					for(int i=0; i<LargeNumUsers.filenameList.size(); i++)
					{
						String filename = LargeNumUsers.filenameList.get(i);
						
						sendRequestFromAFile(filename,
								LargeNumUsers.currRealUnixTime, 
								LargeNumUsers.currRealUnixTime + 
								LargeNumUsers.TIME_UPDATE_SLEEP_TIME);
					}
					
					if( currentEventFileMap.size() > 0)
					{
						logReadLock.notify();
						
						while(currentEventFileMap.size() > 0)
						{
							try 
							{
								logReadLock.wait();
							} 
							catch (InterruptedException e) 
							{
								e.printStackTrace();
							}
						}	
					}
				}
				
				try
				{
					Thread.sleep(LargeNumUsers.TIME_UPDATE_SLEEP_TIME);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
		
		
		private void sendRequestFromAFile( String filename,
				long startTimestamp, long endTimestamp )
		{
			BufferedReader readfile = null;
			
			try
			{
				String sCurrentLine;
				readfile = new BufferedReader(
						new FileReader(LargeNumUsers.USER_TRACE_DIR+"/"+filename));
				
				while( (sCurrentLine = readfile.readLine()) != null )
				{
					try
					{
						JSONObject currJSON = new JSONObject(sCurrentLine);
						
						long jsonTimestamp  = (long)Double.parseDouble(currJSON.getString(
										LargeNumUsers.GEO_LOC_TIME_KEY));
						
						if( (jsonTimestamp >= startTimestamp) 
									&& (jsonTimestamp < endTimestamp) )
						{
							List<JSONObject> eventList = currentEventFileMap.get(filename);
							
							if(eventList == null)
							{
								eventList = new LinkedList<JSONObject>();
								eventList.add(currJSON);
								currentEventFileMap.put(filename, eventList);
							}
							else
							{
								eventList.add(currJSON);
							}
						}
						
						if( jsonTimestamp >= endTimestamp)
						{
							break;
						}
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
				}
				catch (IOException ex)
				{
					ex.printStackTrace();
				}
			}
			//return totalPop;
		}
	}
	
}
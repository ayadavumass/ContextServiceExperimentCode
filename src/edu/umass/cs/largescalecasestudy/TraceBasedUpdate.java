package edu.umass.cs.largescalecasestudy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


import org.json.JSONObject;

import edu.umass.cs.acs.geodesy.GeodeticCalculator;
import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import edu.umass.cs.largescalecasestudy.DistributionLearningFromTraces.DistanceAndAngle;

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
	//private final Object logReadLock			= new Object();
	//private HashMap<String, List<JSONObject>> currentEventFileMap;
	
	
	public TraceBasedUpdate()
	{
		super( LargeNumUsers.UPD_LOSS_TOLERANCE );
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
		while( LargeNumUsers.currRealUnixTime 
							< LargeNumUsers.END_UNIX_TIME )
		{
			int readFileNum  = LargeNumUsers.userinfoFileNum;
			int writeFileNum = (LargeNumUsers.userinfoFileNum+1)%2;
			
			BufferedReader br = null;
			BufferedWriter bw = null;
			try
			{
				String readFileName = LargeNumUsers.USER_INFO_FILE_PREFIX
												+readFileNum;
				
				String writeFileName = LargeNumUsers.USER_INFO_FILE_PREFIX+writeFileNum;
				
				br = new BufferedReader(new FileReader(readFileName));
				bw = new BufferedWriter(new FileWriter(writeFileName));
				
				String currLine;
				while( (currLine = br.readLine()) != null )
				{
					UserRecordInfo userRecInfo = UserRecordInfo.fromString(currLine);					
					
					long currRelativeTime 
						= LargeNumUsers.computeTimeRelativeToDatStart
												(LargeNumUsers.currRealUnixTime);
					
					if( currRelativeTime >= userRecInfo.getNextUpdateUnixTime() )
					{
						// skipping some earlier updates for the day
						// sending only alerts from the last minute or sleep interval.
						if( userRecInfo.getNextUpdateUnixTime() >= 
								(currRelativeTime-(LargeNumUsers.TIME_UPDATE_SLEEP_TIME/1000) ) )
						{
							if( (userRecInfo.getNextUpdateLat() >= LargeNumUsers.MIN_US_LAT) 
									&& (userRecInfo.getNextUpdateLat() <= LargeNumUsers.MAX_US_LAT) 
									&& (userRecInfo.getNextUpdateLong() >= LargeNumUsers.MIN_US_LONG) 
									&& (userRecInfo.getNextUpdateLong() <= LargeNumUsers.MAX_US_LONG) )
							{
								long reqNum = -1;
								synchronized(waitLock)
								{
									numSent++;
									reqNum = numSent;
								}
								
								JSONObject attrValJSON = new JSONObject();
								attrValJSON.put(LargeNumUsers.LATITUDE_KEY, 
													userRecInfo.getNextUpdateLat());
								
								attrValJSON.put(LargeNumUsers.LONGITUDE_KEY, 
													userRecInfo.getNextUpdateLong());
								
								ExperimentUpdateReply updateRep 
										= new ExperimentUpdateReply(reqNum, userRecInfo.getGUID());
								
								LargeNumUsers.csClient.sendUpdateWithCallBack
													(userRecInfo.getGUID(), null, attrValJSON, -1, 
															updateRep, this.getCallBack());
							}
							else
							{
								System.out.println("Update outside area");
							}
						}
						
						// write a next update entry
						if( userRecInfo.getNextUpdateNum() < userRecInfo.getTotalUpdates() )
						{
							int nextUpdateNum   = userRecInfo.getNextUpdateNum()+1;
							assert(nextUpdateNum > 1 );
							
							// for i > 1 nextUpdateTime is relative to previous update
							long nextUpdateTime = DistributionLearningFromTraces.getNextUpdateTimeFromDist
																(userRecInfo.getFilename(), nextUpdateNum);
							
							long reqcurrRelativeTime = LargeNumUsers.computeTimeRelativeToDatStart
									(LargeNumUsers.currRealUnixTime);
							
							// making nextUpdateTime relative to the midnight of the current day
							nextUpdateTime = nextUpdateTime + reqcurrRelativeTime;
							
							nextUpdateTime = LargeNumUsers.distributeTimeUniformly(nextUpdateTime);
							
							
							boolean inTimeslot 
								= LargeNumUsers.checkIfRelativeTimeInTimeSlot(nextUpdateTime);
							
							if( inTimeslot )
							{
								DistanceAndAngle distAngle 
											= DistributionLearningFromTraces.getDistAngleFromDist
																(userRecInfo.getFilename(), nextUpdateNum);
								
								UserRecordInfo nextuserRecInfo = null;
								if(distAngle.distance > 0)
								{
									GlobalCoordinate destCoord 
										= GeodeticCalculator.calculateEndingGlobalCoordinates
											( new GlobalCoordinate(userRecInfo.getHomeLat(), userRecInfo.getHomeLong()), 
															distAngle.angle, distAngle.distance );
									
									nextuserRecInfo = new UserRecordInfo( userRecInfo.getGUID(), 
											userRecInfo.getFilename(), 
											userRecInfo.getHomeLat(), userRecInfo.getHomeLong(), 
											userRecInfo.getTotalUpdates(),
											nextUpdateNum, nextUpdateTime, 
											destCoord.getLatitude(), destCoord.getLongitude() );
								}
								else
								{
									nextuserRecInfo = new UserRecordInfo( userRecInfo.getGUID(), 
										userRecInfo.getFilename(), 
										userRecInfo.getHomeLat(), userRecInfo.getHomeLong(), 
										userRecInfo.getTotalUpdates(),
										nextUpdateNum, nextUpdateTime, 
										userRecInfo.getHomeLat(), userRecInfo.getHomeLong() );
								}
								bw.write(nextuserRecInfo.toString()+"\n");
							}	
						}
						
					}
					else
					{
						// write entry as it is in the file, so that it can eb executed at later time
						bw.write(userRecInfo.toString()+"\n");
					}
				}
			}
			catch(IOException ioex)
			{
				ioex.printStackTrace();
			}
			finally
			{
				if( br != null )
				{
					br.close();
				}
				
				if( bw != null )
				{
					bw.close();
				}
			}
			
			System.out.println("File read loop ends");
			LargeNumUsers.userinfoFileNum = (LargeNumUsers.userinfoFileNum + 1)%2;
			System.out.println("Current userinfo file num "+LargeNumUsers.userinfoFileNum);
			Thread.sleep(LargeNumUsers.TIME_UPDATE_SLEEP_TIME);
		}
		
		System.out.println("Trace based request sending ends");
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
			
			if(numRecvd%10 == 0)
			{
				System.out.println("AverageUpdateLatency "+getAverageUpdateLatency()
				                   +" NumUpdatesRecvd "+getNumUpdatesRecvd());
			}
			
//			System.out.println("AverageUpdateLatency "+getAverageUpdateLatency()
//            			+" NumUpdatesRecvd "+getNumUpdatesRecvd());
			
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
}
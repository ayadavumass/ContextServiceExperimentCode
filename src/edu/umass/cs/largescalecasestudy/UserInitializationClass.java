package edu.umass.cs.largescalecasestudy;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.json.JSONObject;

import com.google.common.hash.Hashing;


public class UserInitializationClass extends AbstractRequestSendingClass
{
	// different random generator for each variable, as using one for all of them
	// doesn't give uniform properties.
	private final Random initRand;
	
	public UserInitializationClass()
	{
		super( LargeNumUsers.INSERT_LOSS_TOLERANCE );
		initRand = new Random((LargeNumUsers.myID+1)*100);	
	}
	
	private void sendAInitMessage(long guidNum, BufferedWriter bw) throws Exception
	{
		/*double randnum = initRand.nextDouble();
		
		CountyNode countynode = LargeNumUsers.binarySearchOfCounty(randnum);
			
		double homeLat =  countynode.minLat + 
					(countynode.maxLat - countynode.minLat) * initRand.nextDouble();
		
		double homeLong = countynode.minLong + 
					(countynode.maxLong - countynode.minLong) * initRand.nextDouble();
		
		String userGUID = LargeNumUsers.getSHA1(LargeNumUsers.guidPrefix+guidNum);
		
		int arrayIndex = Hashing.consistentHash(userGUID.hashCode(), 
				LargeNumUsers.filenameList.size());
		
		String filename = LargeNumUsers.filenameList.get(arrayIndex);
		int numUpdatesPerDay = DistributionLearningFromTraces.getNumUpdatesFromDistForUser
																					(filename);
		int nextUpdateNum   = 1;
		long nextUpdateTime = DistributionLearningFromTraces.getNextUpdateTimeFromDist
																(filename, nextUpdateNum);
		
		nextUpdateTime = LargeNumUsers.distributeTimeUniformly(nextUpdateTime);
		
		boolean inTimeslot = LargeNumUsers.checkIfRelativeTimeInTimeSlot(nextUpdateTime);
		
		//DistanceAndAngle distAngle 
		//				= DistributionLearningFromTraces.getDistAngleFromDist
		//												(filename, nextUpdateNum);
		//GlobalCoordinate destCoord = GeodeticCalculator.calculateEndingGlobalCoordinates
		//		( new GlobalCoordinate(homeLat, homeLong), 
		//				distAngle.angle, distAngle.distance );
		
		if(inTimeslot)
		{
			UserRecordInfo userRecInfo = new UserRecordInfo( userGUID, filename, 
				homeLat, homeLong, numUpdatesPerDay,
				nextUpdateNum, nextUpdateTime, 
				homeLat, homeLong );
			
			try
			{
				bw.write(userRecInfo.toString() +"\n");
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		
		JSONObject attrValJSON = new JSONObject();
		
		attrValJSON.put(LargeNumUsers.LATITUDE_KEY, homeLat);
		attrValJSON.put(LargeNumUsers.LONGITUDE_KEY, homeLong);
		
		ExperimentUpdateReply updateRep = new ExperimentUpdateReply(guidNum, userGUID);
		
		LargeNumUsers.csClient.sendUpdateWithCallBack
										( userGUID, null, attrValJSON, 
										 -1, updateRep, this.getCallBack() );*/
	}
	
	public void initializaRateControlledRequestSender() throws Exception
	{
		this.startExpTime();
		double reqspms = LargeNumUsers.initRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		long totalNumUsersSent = 0;
		BufferedWriter bw = null;
		
		try
		{
			String writeFileName = LargeNumUsers.USER_INFO_FILE_PREFIX
												+LargeNumUsers.userinfoFileNum;
			
			bw = new BufferedWriter(new FileWriter(writeFileName));
			
			while(  totalNumUsersSent < LargeNumUsers.numusers  )
			{
				for(int i=0; i<numberShouldBeSentPerSleep; i++ )
				{
					sendAInitMessage(totalNumUsersSent, bw);
					totalNumUsersSent++;
					numSent++;
					assert(numSent == totalNumUsersSent);
					if(totalNumUsersSent >= LargeNumUsers.numusers)
					{
						break;
					}
				}
				if(totalNumUsersSent >= LargeNumUsers.numusers)
				{
					break;
				}
				currTime = System.currentTimeMillis();
				
				double timeElapsed = ((currTime- expStartTime)*1.0);
				double numberShouldBeSentByNow = timeElapsed*reqspms;
				double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
				if(needsToBeSentBeforeSleep > 0)
				{
					needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
				}
				
				for(int i=0;i<needsToBeSentBeforeSleep;i++)
				{
					sendAInitMessage(totalNumUsersSent, bw);
					totalNumUsersSent++;
					numSent++;
					assert(numSent == totalNumUsersSent);
					if(totalNumUsersSent >= LargeNumUsers.numusers)
					{
						break;
					}
				}
				
				if(totalNumUsersSent >= LargeNumUsers.numusers)
				{
					break;
				}
				Thread.sleep(100);
			}
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
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("UserInit eventual sending rate "+sendingRate);
		
		waitForFinish();
		
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("UserInit result:Goodput "+sysThrput);	
	}
	
	
	public void writeTraceToFile(int numEntries)
	{
		/*BufferedWriter bw = null;
		
		try 
		{
			bw = new BufferedWriter(new FileWriter("nationwidePopTrace.txt"));
			
			for(int i=0; i<numEntries; i++)
			{	
				double randnum = initRand.nextDouble();
				
				CountyNode countynode = LargeNumUsers.binarySearchOfCounty(randnum);
				
				
				double latitude =  countynode.minLat + 
							(countynode.maxLat - countynode.minLat) * initRand.nextDouble();
				
				double longitude = countynode.minLong + 
							(countynode.maxLong - countynode.minLong) * initRand.nextDouble();
				
				bw.write("latitude,"+latitude+",longitude,"+longitude+"\n");
			}
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		} finally 
		{
			try 
			{
				if (bw != null)
					bw.close();
			}
			catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}*/
	}
	
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			if(numRecvd%10000 == 0)
			{
				System.out.println("UserInit reply recvd "+userGUID+" time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
			}
			//if(currNumReplyRecvd == currNumReqSent)
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken)
	{
	}
}
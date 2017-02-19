package edu.umass.cs.largescalecasestudy;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.json.JSONObject;

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
	
	private void sendAInitMessage(int guidNum) throws Exception
	{
		double randnum = initRand.nextDouble();
		
		CountyNode countynode = LargeNumUsers.binarySearchOfCounty(randnum);
		
		
		double latitude =  countynode.minLat + 
					(countynode.maxLat - countynode.minLat) * initRand.nextDouble();
		
		double longitude = countynode.minLong + 
					(countynode.maxLong - countynode.minLong) * initRand.nextDouble();
		
		
		JSONObject attrValJSON = new JSONObject();
		
		attrValJSON.put("latitude", latitude);
		attrValJSON.put("longitude", longitude);
		
		
		String userGUID = LargeNumUsers.getSHA1(LargeNumUsers.guidPrefix+guidNum);
		
		ExperimentUpdateReply updateRep = new ExperimentUpdateReply(guidNum, userGUID);
		
		LargeNumUsers.csClient.sendUpdateWithCallBack
										( userGUID, null, attrValJSON, 
										 -1, updateRep, this.getCallBack() );
	}
	
	
	public void initializaRateControlledRequestSender() throws Exception
	{	
		this.startExpTime();
		double reqspms = LargeNumUsers.initRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double totalNumUsersSent = 0;
		
		while(  totalNumUsersSent < LargeNumUsers.numusers  )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendAInitMessage((int)totalNumUsersSent);
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
				sendAInitMessage((int)totalNumUsersSent);
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
		BufferedWriter bw = null;
		
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
		}
	}
	
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("UserInit reply recvd "+userGUID+" time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
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
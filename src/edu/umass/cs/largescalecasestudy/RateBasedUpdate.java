package edu.umass.cs.largescalecasestudy;

import java.nio.ByteBuffer;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.utils.Utils;


public class RateBasedUpdate extends 
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
	
	private Random uniformRand;
	
	public RateBasedUpdate()
	{
		super( LargeNumUsers.UPD_LOSS_TOLERANCE );
		uniformRand = new Random((LargeNumUsers.myID+1)*100);
	}
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			uniformRequestSender();
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	private void uniformRequestSender() throws Exception
	{
		double reqspms = LargeNumUsers.requestsps/1000.0;
		long currTime  = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		//double currUserGuidNum   = 0;
		long currGuidNum;
		
		//while( ( totalNumUsersSent < numUsers ) )
		while( ( (System.currentTimeMillis() - expStartTime) 
						< 100000 ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				currGuidNum = LargeNumUsers.numusers*LargeNumUsers.myID +  (long)Math.floor
						(uniformRand.nextDouble()*LargeNumUsers.numusers);
				
				doUpdate(currGuidNum);
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
				currGuidNum = LargeNumUsers.numusers*LargeNumUsers.myID +  (long)Math.floor
						(uniformRand.nextDouble()*LargeNumUsers.numusers);
				
				doUpdate(currGuidNum);
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Update eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Update result:Goodput "+sysThrput);
	}
	
	
	private void doUpdate(long currUserGuidNum) throws JSONException
	{
		numSent++;
		String guid = getOrderedHash(currUserGuidNum);
		
		JSONObject updateJSON = new JSONObject();
		
		String attrName = LargeNumUsers.LATITUDE_KEY;
		double value = LargeNumUsers.MIN_US_LAT 
				+ uniformRand.nextDouble()*(LargeNumUsers.MAX_US_LAT-LargeNumUsers.MIN_US_LAT);
		
		updateJSON.put(attrName, value);
		
		
		attrName = LargeNumUsers.LONGITUDE_KEY;
		
		value = LargeNumUsers.MIN_US_LONG 
				+ uniformRand.nextDouble()*(LargeNumUsers.MAX_US_LONG-LargeNumUsers.MIN_US_LONG);
		
		updateJSON.put(attrName, value);
		
		
		ExperimentUpdateReply updateRep 
			= new ExperimentUpdateReply(numSent, guid);

		LargeNumUsers.csClient.sendUpdateWithCallBack
					(guid, null, updateJSON, -1, 
							updateRep, this.getCallBack());
	}
	
	
	public static String getOrderedHash(long guidNum)
	{
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(guidNum);
	    String hexArray = Utils.byteArrayToHex(buffer.array());
	    
	    while(hexArray.length() < 40)
	    {
	    	hexArray = "0"+hexArray ;
	    }
		return hexArray;
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
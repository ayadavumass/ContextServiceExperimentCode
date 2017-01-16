package edu.umass.cs.ubercasestudy;

import java.util.Random;

import org.json.JSONObject;


public class InitializeTaxisClass extends AbstractRequestSendingClass
{
	public static final double INSERT_LOSS_TOLERANCE 	= 0.0;
	public static final double INIT_RATE				= 100.0;
	public static final int WAIT_TIME					= 100000000;
	
	// different random generator for each variable, as using one for all of them
	// doesn't give uniform properties.
	private final Random initRand;
	
	public InitializeTaxisClass()
	{
		super( INSERT_LOSS_TOLERANCE, WAIT_TIME );
		initRand = new Random(Driver.myID*100);
	}
	
	private void sendAInitMessage(int guidNum) throws Exception
	{
		JSONObject attrValJSON = new JSONObject();
		
		// for lat 
		double latDiff    = Driver.MAX_LAT - Driver.MIN_LAT;	
		double longDiff   = Driver.MAX_LONG - Driver.MIN_LONG;
		double latVal     = Driver.MIN_LAT + latDiff * initRand.nextDouble();
		double longVal    = Driver.MIN_LONG + longDiff * initRand.nextDouble();
		
		
		attrValJSON.put( Driver.LAT_ATTR, latVal );
		attrValJSON.put( Driver.LONG_ATTR, longVal );
		attrValJSON.put( Driver.STATUS_ATTR, Driver.FREE_TAXI_STATUS );
		
		
		String accountAlias = Driver.GUID_PREFIX+guidNum+"@gmail.com";
		
		String guid = Driver.getSHA1(accountAlias);
		
		synchronized(Driver.taxiFreeMap)
		{
			Driver.taxiFreeMap.put(guid, true);
		}
		
		ExperimentUpdateReply updateRep = new ExperimentUpdateReply(guidNum, guid);
		
		Driver.csClient.sendUpdateWithCallBack
										( guid, null, 
										attrValJSON, -1, updateRep, this.getCallBack() );	
	}
	
	
	public void initializaRateControlledRequestSender() throws Exception
	{
		this.startExpTime();
		double reqspms = INIT_RATE/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double totalNumUsersSent = 0;
		
		while( totalNumUsersSent < Driver.NUMBER_TAXIS )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendAInitMessage((int)totalNumUsersSent);
				totalNumUsersSent++;
				numSent++;
				assert(numSent == totalNumUsersSent);
				if( totalNumUsersSent >= Driver.NUMBER_TAXIS )
				{
					break;
				}
			}
			if( totalNumUsersSent >= Driver.NUMBER_TAXIS )
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
				if( totalNumUsersSent >= Driver.NUMBER_TAXIS )
				{
					break;
				}
			}
			if( totalNumUsersSent >= Driver.NUMBER_TAXIS )
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
	
	
	@Override
	public void incrementUpdateNumRecvd(ExperimentUpdateReply expUpdateReply) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("UserInit reply recvd "+expUpdateReply.getGuid()
					+" time taken "+expUpdateReply.getCompletionTime()+
					" numSent "+numSent+" numRecvd "+numRecvd);
			
			if(checkForCompletionWithLossTolerance())
			{
				waitLock.notify();
			}
		}
		
	}

	@Override
	public void incrementSearchNumRecvd(ExperimentSearchReply expSearchReply) 
	{
		
	}
	
	
//	@Override
//	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
//	{
//		synchronized(waitLock)
//		{
//			numRecvd++;
//			System.out.println("UserInit reply recvd "+userGUID+" time taken "+timeTaken+
//					" numSent "+numSent+" numRecvd "+numRecvd);
//			
//			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
//			{
//				waitLock.notify();
//			}
//		}
//	}
}
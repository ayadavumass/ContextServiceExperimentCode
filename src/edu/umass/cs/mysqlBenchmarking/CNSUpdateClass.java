package edu.umass.cs.mysqlBenchmarking;


import java.nio.ByteBuffer;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.utils.Utils;
/**
 * Updates locations of all users after every 
 * granularityOfGeolocationUpdate
 * @author adipc
 */
public class CNSUpdateClass extends AbstractRequestSendingClass
{
	public static final double MIN_US_LAT						= 22.0;
	public static final double MAX_US_LAT						= 48.0;
	
	public static final double MIN_US_LONG						= -125.0;
	public static final double MAX_US_LONG						= -66.0;
	
	// latitude longitude key in json and attribute names in CNS
	public static final String LATITUDE_KEY						= "latitude";
	public static final String LONGITUDE_KEY					= "longitude";
	
	
	private Random updateRand;
	
	private double sumUpdTime = 0;
	
	public CNSUpdateClass()
	{
		super(MySQLThroughputBenchmarking.UPD_LOSS_TOLERANCE);
		updateRand = new Random();
	}
	
	
	@Override
	public void run()
	{
		try 
		{
			this.startExpTime();
			updRateControlledRequestSender();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	private void updRateControlledRequestSender() throws Exception
	{
		double reqspms = MySQLThroughputBenchmarking.requestsps/1000.0;
		long currTime  = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		//double currUserGuidNum   = 0;
		long currGuidNum;
		
		//while( ( totalNumUsersSent < numUsers ) )
		while( ( (System.currentTimeMillis() - expStartTime) 
						< MySQLThroughputBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				currGuidNum = (long)Math.floor
						(updateRand.nextDouble()*MySQLThroughputBenchmarking.numGuids);
				
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
				currGuidNum = (long)Math.floor
						(updateRand.nextDouble()*MySQLThroughputBenchmarking.numGuids);
				
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
		
		String attrName = LATITUDE_KEY;
		double value = updateRand.nextDouble()*(MAX_US_LAT-MIN_US_LAT);
		
		updateJSON.put(attrName, value);
		
		
		attrName = LONGITUDE_KEY;
		value = updateRand.nextDouble()*(MAX_US_LONG-MIN_US_LONG);
		
		updateJSON.put(attrName, value);
		
		CNSUpdateTask updTask = new CNSUpdateTask( guid, updateJSON, this);
		MySQLThroughputBenchmarking.taskES.execute(updTask);
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
	

	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			sumUpdTime = sumUpdTime+timeTaken;
//			System.out.println("Update reply recvd "+userGUID+" time taken "+timeTaken+
//					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
	public double getAvgUpdateTime()
	{
		return sumUpdTime/numRecvd;
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken) 
	{
	}
}
package edu.umass.cs.genericExpClientCallBack;

import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class BothSearchAndUpdate extends 
					AbstractRequestSendingClass implements Runnable
{
	private final Random generalRand;
	private Random searchQueryRand;
	private final Random updateRand;
	
	private double currUserGuidNum   		= 0;
	
	// we don't want to issue new search queries for the trigger exp.
	// so that the number of search queries in the experiment remains same.
	// so when number of search queries reaches threshold then we reset it to 
	// the beginning.
	//private long numberSearchesSent		= 0;
	
	public BothSearchAndUpdate()
	{
		super( SearchAndUpdateDriver.UPD_LOSS_TOLERANCE );
		generalRand = new Random(SearchAndUpdateDriver.myID);
		updateRand = new Random(SearchAndUpdateDriver.myID*100);
		
		searchQueryRand = new Random(SearchAndUpdateDriver.myID*200);
	}
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			
			if( !SearchAndUpdateDriver.singleRequest )
			{
				rateControlledRequestSender();
			}
			else
			{
				singleRequestSender();
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void rateControlledRequestSender() throws Exception
	{
		double reqsps = SearchAndUpdateDriver.updateRate;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqsps;
		
		while( ( (System.currentTimeMillis() - expStartTime) < 
				SearchAndUpdateDriver.EXPERIMENT_TIME ) )
		{
			for( int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendRequest(numSent);
				numSent++;
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0);
			double numberShouldBeSentByNow = (timeElapsed*reqsps)/1000.0;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
				sendRequest(numSent);
				numSent++;
			}
			Thread.sleep(1000);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Both eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Both result:Goodput "+sysThrput);
	}
	
	/**
	 * This is mainly used to measure very low load latency
	 */
	private void singleRequestSender()
	{
		System.out.println("singleRequestSender used");
		while( 
		( (System.currentTimeMillis() - expStartTime) < SearchAndUpdateDriver.EXPERIMENT_TIME ) )
		{	
			if( generalRand.nextDouble() 
					< SearchAndUpdateDriver.rhoValue )
			{
				sendSingleSearch();
			}
			else
			{
				sendSingleUpdate();
			}
			
			numSent++;
			try
			{
				Thread.sleep(500);
			} 
			catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	private void sendSingleUpdate()
	{
		String userGUID = "";
		
		userGUID = SearchAndUpdateDriver.getSHA1
					(SearchAndUpdateDriver.guidPrefix+currUserGuidNum);
		
		
		int randomAttrNum = updateRand.nextInt(SearchAndUpdateDriver.numAttrs);
		double randVal = SearchAndUpdateDriver.ATTR_MIN 
				+updateRand.nextDouble()*
				(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN);
		
		JSONObject attrValJSON = new JSONObject();
		try
		{			
			attrValJSON.put(SearchAndUpdateDriver.attrPrefix+randomAttrNum, randVal);
		} 
		catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		UpdateTask updTask = new UpdateTask( attrValJSON, userGUID, this );
		updTask.run();
		
		currUserGuidNum++;
		currUserGuidNum=((int)currUserGuidNum)%SearchAndUpdateDriver.numUsers;
	}
	
	private void sendSingleSearch()
	{
		String searchQuery
			= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE ";
//		+ "geoLocationCurrentLat >= "+latitudeMin +" AND geoLocationCurrentLat <= "+latitudeMax 
//		+ " AND "
//		+ "geoLocationCurrentLong >= "+longitudeMin+" AND geoLocationCurrentLong <= "+longitudeMax;
		int randAttrNum = -1;
		for( int i=0;i<SearchAndUpdateDriver.numAttrsInQuery;i++)
		{
			// if num attrs and num in query are same then send query on all attrs
			if(SearchAndUpdateDriver.numAttrs == SearchAndUpdateDriver.numAttrsInQuery)
			{
				randAttrNum++;
			}
			else
			{
				randAttrNum = searchQueryRand.nextInt(SearchAndUpdateDriver.numAttrs);
			}
			
			String attrName = SearchAndUpdateDriver.attrPrefix+randAttrNum;
			double attrMin 
				= SearchAndUpdateDriver.ATTR_MIN
				+searchQueryRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN);
		
			double predLength 
				= (searchQueryRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN));
		
			double attrMax = attrMin + predLength;
			//		double latitudeMax = latitudeMin 
			//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
			// making it curcular
			if( attrMax > SearchAndUpdateDriver.ATTR_MAX )
			{
				double diff = attrMax - SearchAndUpdateDriver.ATTR_MAX;
				attrMax = SearchAndUpdateDriver.ATTR_MIN + diff;
			}
			// last so no AND
			if(i == (SearchAndUpdateDriver.numAttrsInQuery-1))
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
						+" <= "+attrMax;
			}
			else
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
					+" <= "+attrMax+" AND ";
			}
		}
		SearchTask searchTask = new SearchTask( searchQuery, new JSONArray(), this );
		searchTask.run();
	}
	
	private void sendRequest( long reqIdNum )
	{
		// send update
		if( generalRand.nextDouble() < SearchAndUpdateDriver.rhoValue )
		{
//			numberSearchesSent++;
//			if( numberSearchesSent > 
//			(SearchAndUpdateDriver.searchQueryRate * (SearchAndUpdateDriver.EXPERIMENT_TIME/1000.0)) )
//			{
//				numberSearchesSent = 0;
//				// reinitialize rand number so that it gives the same seq again.
//				searchQueryRand = new Random(SearchAndUpdateDriver.myID*200);
//			}
			//sendQueryMessage();
			sendQueryMessageWithSmallRanges(reqIdNum);
		}
		else
		{
			sendUpdate(reqIdNum);
		}
	}
	
	private void sendUpdate(long reqIdNum)
	{
		sendALocMessage((int)currUserGuidNum, reqIdNum);
		currUserGuidNum++;
		currUserGuidNum=((int)currUserGuidNum)%SearchAndUpdateDriver.numUsers;
	}
	
	private void sendQueryMessage(long reqIdNum)
	{
		String searchQuery
			= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE ";
//			+ "geoLocationCurrentLat >= "+latitudeMin +" AND geoLocationCurrentLat <= "+latitudeMax 
//			+ " AND "
//			+ "geoLocationCurrentLong >= "+longitudeMin+" AND geoLocationCurrentLong <= "+longitudeMax;
		
		int randAttrNum = -1;
		for( int i=0; i<SearchAndUpdateDriver.numAttrsInQuery; i++)
		{
			// if num attrs and num in query are same then send query on all attrs
			if(SearchAndUpdateDriver.numAttrs == SearchAndUpdateDriver.numAttrsInQuery)
			{
				randAttrNum++;
			}
			else
			{
				randAttrNum = searchQueryRand.nextInt(SearchAndUpdateDriver.numAttrs);
			}				
			
			String attrName = SearchAndUpdateDriver.attrPrefix+randAttrNum;
			double attrMin 
				= SearchAndUpdateDriver.ATTR_MIN
				+searchQueryRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN);
			
			double predLength 
				= (searchQueryRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN));
			
			double attrMax = attrMin + predLength;
			//		double latitudeMax = latitudeMin 
			//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
			// making it curcular
			if( attrMax > SearchAndUpdateDriver.ATTR_MAX )
			{
				double diff = attrMax - SearchAndUpdateDriver.ATTR_MAX;
				attrMax = SearchAndUpdateDriver.ATTR_MIN + diff;
			}
			// last so no AND
			if(i == (SearchAndUpdateDriver.numAttrsInQuery-1))
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
						+" <= "+attrMax;
			}
			else
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
					+" <= "+attrMax+" AND ";
			}
		}	
//		SearchTask searchTask = new SearchTask( searchQuery, new JSONArray(), this );
//		SearchAndUpdateDriver.taskES.execute(searchTask);
		
		ExperimentSearchReply searchRep 
					= new ExperimentSearchReply( reqIdNum );
		
		SearchAndUpdateDriver.csClient.sendSearchQueryWithCallBack
					(searchQuery, 300000, searchRep, this.getCallBack());
	}
	
	private void sendQueryMessageWithSmallRanges(long reqIdNum)
	{
		String searchQuery
			= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE ";
		
		int randAttrNum = -1;
		for( int i=0; i<SearchAndUpdateDriver.numAttrsInQuery; i++)
		{
			// if num attrs and num in query are same then send query on all attrs
			if(SearchAndUpdateDriver.numAttrs == SearchAndUpdateDriver.numAttrsInQuery)
			{
				randAttrNum++;
			}
			else
			{
				randAttrNum = searchQueryRand.nextInt(SearchAndUpdateDriver.numAttrs);
			}
						
			
			String attrName = SearchAndUpdateDriver.attrPrefix+randAttrNum;
			double attrMin 
				= SearchAndUpdateDriver.ATTR_MIN
				+searchQueryRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN);
			
			// querying 10 % of domain
			double predLength 
				= (0.1*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN)) ;
			
			double attrMax = attrMin + predLength;
			//		double latitudeMax = latitudeMin 
			//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
			// making it curcular
			if( attrMax > SearchAndUpdateDriver.ATTR_MAX )
			{
				double diff = attrMax - SearchAndUpdateDriver.ATTR_MAX;
				attrMax = SearchAndUpdateDriver.ATTR_MIN + diff;
			}
			// last so no AND
			if(i == (SearchAndUpdateDriver.numAttrsInQuery-1))
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
						+" <= "+attrMax;
			}
			else
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
					+" <= "+attrMax+" AND ";
			}
		}
		
		ExperimentSearchReply searchRep 
				= new ExperimentSearchReply( reqIdNum );

		SearchAndUpdateDriver.csClient.sendSearchQueryWithCallBack
			(searchQuery, 300000, searchRep, this.getCallBack());
		
//		SearchTask searchTask = new SearchTask( searchQuery, new JSONArray(), this );
//		SearchAndUpdateDriver.taskES.execute(searchTask);
	}
	
	
	private void sendALocMessage(int currUserGuidNum, long reqIdNum)
	{
		String userGUID = "";
		if( SearchAndUpdateDriver.useGNS )
		{
//			userGUID = userGuidEntry.getGuid();
		}
		else
		{
			userGUID = SearchAndUpdateDriver.getSHA1
					(SearchAndUpdateDriver.guidPrefix+currUserGuidNum);
		}
		
		int randomAttrNum = updateRand.nextInt(SearchAndUpdateDriver.numAttrs);
		double randVal = SearchAndUpdateDriver.ATTR_MIN 
				+updateRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN);
		
		JSONObject attrValJSON = new JSONObject();
		try
		{
			attrValJSON.put(SearchAndUpdateDriver.attrPrefix+randomAttrNum, randVal);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		ExperimentUpdateReply updateRep = new ExperimentUpdateReply(reqIdNum, userGUID);
		
		SearchAndUpdateDriver.csClient.sendUpdateWithCallBack
										(userGUID, null, 
										attrValJSON, -1, updateRep, this.getCallBack());
		//SearchAndUpdateDriver.csClient.sendUpdate(userGUID, null, 
		//		attrValuePairs, -1);
		//System.out.println("Updating "+currUserGuidNum+" "+attrValJSON);
//		UpdateTask updTask = new UpdateTask( attrValJSON, userGUID, this );
//		SearchAndUpdateDriver.taskES.execute(updTask);
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("LocUpd reply recvd "+userGUID+" time taken "+timeTaken+
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
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("Search reply recvd size "+resultSize+" time taken "
					+timeTaken+" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
}
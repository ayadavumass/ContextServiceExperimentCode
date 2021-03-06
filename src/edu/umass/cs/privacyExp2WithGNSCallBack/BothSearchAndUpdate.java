package edu.umass.cs.privacyExp2WithGNSCallBack;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.util.GuidEntry;

public class BothSearchAndUpdate extends 
						AbstractRequestSendingClass implements Runnable
{
	private final Random generalRand;
	private Random searchQueryRand;
	private final Random updateRand;
	
	private double currUserGuidNum   			= 0;
	
	private double sumResultSize				= 0.0;
	private double sumSearchTimeTaken			= 0.0;
	private double sumUpdateTimeTaken			= 0.0;
	private long numSearchesRecvd				= 1;
	private long numUpdatesRecvd				= 1;
	
	
	// we don't want to issue new search queries for the trigger exp.
	// so that the number of search queries in the experiment remains same.
	// so when number of search queries reaches threshold then we reset it to 
	// the beginning.
	
	public BothSearchAndUpdate()
	{
		super( SearchAndUpdateDriver.UPD_LOSS_TOLERANCE );
		generalRand = new Random(SearchAndUpdateDriver.myID);
		updateRand = new Random((SearchAndUpdateDriver.myID+1)*100);
		
		searchQueryRand = new Random((SearchAndUpdateDriver.myID+1)*200);
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
	
	private void rateControlledRequestSender() throws Exception
	{
		double reqsps = SearchAndUpdateDriver.updateRate;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqsps;
		
		//while( ( totalNumUsersSent < numUsers ) )
		while( ( (System.currentTimeMillis() - expStartTime) < 
								SearchAndUpdateDriver.EXPERIMENT_TIME ) )
		{
			for( int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendRequest(numSent);
				numSent++;
			}
			
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)/1000.0);
			double numberShouldBeSentByNow = timeElapsed*reqsps;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0; i<needsToBeSentBeforeSleep; i++)
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
	
	private void sendRequest(long currReqId)
	{
		// send update
		if( generalRand.nextDouble() 
				< SearchAndUpdateDriver.rhoValue )
		{
//			numberSearchesSent++;
//			if( numberSearchesSent > 
//			(SearchAndUpdateDriver.searchQueryRate * (SearchAndUpdateDriver.EXPERIMENT_TIME/1000.0)) )
//			{
//				numberSearchesSent = 0;
//				// reinitialize rand number so that it gives the same seq again.
//				searchQueryRand = new Random(SearchAndUpdateDriver.myID*200);
//			}
			//sendQueryMessage(currReqId);
			sendQueryMessageWithSmallRanges(currReqId);
		}
		else
		{
			sendUpdate(currReqId);
		}
	}
	
	
	private void sendUpdate( long currReqId )
	{
		sendALocMessage((int)currUserGuidNum, currReqId);
		currUserGuidNum++;
		currUserGuidNum=((int)currUserGuidNum)%SearchAndUpdateDriver.numUsers;
	}
	
	private void sendQueryMessageWithSmallRanges(long currReqId)
	{
		String searchQuery = "";
		
		HashMap<String, Boolean> distinctAttrMap 
			= pickDistinctAttrs( SearchAndUpdateDriver.numAttrsInQuery, 
					SearchAndUpdateDriver.numAttrs, searchQueryRand );
		
		Iterator<String> attrIter = distinctAttrMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			double attrMin = SearchAndUpdateDriver.ATTR_MIN
					+searchQueryRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX 
							- SearchAndUpdateDriver.ATTR_MIN);
		
			// querying 10 % of domain
			double predLength 
				= (SearchAndUpdateDriver.predicateLength
						*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN)) ;
		
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
			if( !attrIter.hasNext() )
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
//		ExperimentSearchReply searchRep 
//				= new ExperimentSearchReply( reqIdNum );
//		
//		SearchAndUpdateDriver.csClient.sendSearchQueryWithCallBack
//			( searchQuery, 300000, searchRep, this.getCallBack() );
		
		int randIndex 
					= searchQueryRand.nextInt( SearchAndUpdateDriver.usersVector.size() );
		
		UserEntry queryingUserEntry 
					= SearchAndUpdateDriver.usersVector.get(randIndex);
	
		GuidEntry queryingGuidEntry = queryingUserEntry.getGuidEntry();
	
		ExperimentSearchReply searchRep 
			= new ExperimentSearchReply( currReqId );

		SearchAndUpdateDriver.csClient.sendSearchQuerySecureWithCallBack
			(searchQuery, 300000, queryingGuidEntry, searchRep, this.getCallBack());
	}
	
	private HashMap<String, Boolean> pickDistinctAttrs( int numAttrsToPick, 
			int totalAttrs, Random randGen )
	{
		HashMap<String, Boolean> hashMap = new HashMap<String, Boolean>();
		int currAttrNum = 0;
		while(hashMap.size() != numAttrsToPick)
		{
			if(SearchAndUpdateDriver.numAttrs == SearchAndUpdateDriver.numAttrsInQuery)
			{
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
				currAttrNum++;
			}
			else
			{
				currAttrNum = randGen.nextInt(SearchAndUpdateDriver.numAttrs);
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
			}
		}
		return hashMap;
	}
	
	private void sendALocMessage( int currUserGuidNum, long currReqId )
	{
		UserEntry currUserEntry 
					= SearchAndUpdateDriver.usersVector.get(currUserGuidNum);
		
		int randomAttrNum = updateRand.nextInt(SearchAndUpdateDriver.numAttrs);
		double randVal = SearchAndUpdateDriver.ATTR_MIN 
				+updateRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX 
						- SearchAndUpdateDriver.ATTR_MIN);
		
		JSONObject attrValJSON = new JSONObject();
		
		try
		{		
			attrValJSON.put(SearchAndUpdateDriver.attrPrefix+randomAttrNum, randVal);
		} 
		catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		GuidEntry myGUIDInfo = currUserEntry.getGuidEntry();
		String guidString = currUserEntry.getGuidEntry().getGuid();
		
		ExperimentUpdateReply updateRep 
					= new ExperimentUpdateReply(currReqId, guidString);
		
		SearchAndUpdateDriver.csClient.sendUpdateSecureWithCallback
						( guidString, myGUIDInfo, attrValJSON, -1, 
								currUserEntry.getACLMap(), 
								currUserEntry.getAnonymizedIDList(),
								updateRep, this.getCallBack() );
//		System.out.println("sendUpdateSecureWithCallback time "
//								+(System.currentTimeMillis()-start));
		
//		UpdateTask updTask = new UpdateTask( attrValJSON, currUserEntry, this );
//		SearchAndUpdateDriver.taskES.execute(updTask);
	}
	
	public void printStats()
	{
		String str = "Num search replies "+ this.numSearchesRecvd 
				+" Num update replies "+ this.numUpdatesRecvd
				+" Avg search size "+ (this.sumResultSize/this.numSearchesRecvd)
				+" Avg search time "+ (this.sumSearchTimeTaken/this.numSearchesRecvd)
				+" Avg update time "+ (this.sumUpdateTimeTaken/this.numUpdatesRecvd);
		System.out.println("Stats "+str);
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			this.sumUpdateTimeTaken = this.sumUpdateTimeTaken + timeTaken;
			numUpdatesRecvd++;
//			System.out.println("LocUpd reply recvd "+userGUID+" time taken "+timeTaken+
//					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
	
	@Override
	public void incrementSearchNumRecvd( int resultSize, long timeTaken )
	{
		synchronized( waitLock )
		{
			numRecvd++;
			this.sumResultSize = this.sumResultSize + resultSize;
			this.sumSearchTimeTaken = this.sumSearchTimeTaken + timeTaken;
			numSearchesRecvd++;
//			System.out.println("Search reply recvd size "+resultSize+" time taken "+timeTaken+
//					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
	
}
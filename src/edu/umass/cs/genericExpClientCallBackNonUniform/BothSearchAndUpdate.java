package edu.umass.cs.genericExpClientCallBackNonUniform;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

public class BothSearchAndUpdate extends 
					AbstractRequestSendingClass implements Runnable
{
	private final Random generalRand;
	private Random searchQueryRand;
	private final Random updateRand;
	
	private long sumResultSize				= 0;
	
	private long sumSearchLatency			= 0;
	private long sumUpdateLatency			= 0;
	
	private long numSearchesRecvd			= 0;
	private long numUpdatesRecvd			= 0;
	
	
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
			
			rateControlledRequestSender();
			
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
		
		double avgResultSize = 0;
		if( this.numSearchesRecvd > 0 )
		{
			avgResultSize = (sumResultSize/this.numSearchesRecvd);
		}
		
		System.out.println("Both result:Goodput "+sysThrput+" average resultsize "
										+avgResultSize);
	}
	
	
	private void sendRequest( long reqIdNum )
	{
		// send update
		if( generalRand.nextDouble() < SearchAndUpdateDriver.rhoValue )
		{
			sendQueryMessageWithSmallRanges(reqIdNum);
		}
		else
		{
			sendUpdate(reqIdNum);
		}
	}
	
	private void sendUpdate(long reqIdNum)
	{
		sendALocMessage( reqIdNum);
	}
	
	private void sendQueryMessageWithSmallRanges(long reqIdNum)
	{
		String searchQuery = createSearchQuery();
		
		ExperimentSearchReply searchRep 
				= new ExperimentSearchReply( reqIdNum );
		
		SearchAndUpdateDriver.csClient.sendSearchQueryWithCallBack
			( searchQuery, SearchAndUpdateDriver.queryExpiryTime, searchRep, this.getCallBack() );
	}
	
	
	private String createSearchQuery()
	{
		String searchQuery = "";
		
		
		HashMap<String, Boolean> distinctAttrMap 
			= pickDistinctAttrs( SearchAndUpdateDriver.numAttrsInQuery
					, searchQueryRand );
		
		Iterator<String> attrIter = distinctAttrMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			double attrMin = SearchAndUpdateDriver.convertGuassianIntoValInRange
							(searchQueryRand);
			
			// querying 10 % of domain
			double predLength 
				= (SearchAndUpdateDriver.predicateLengthRatio
						*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN));
			
			double attrMax = Math.min(attrMin + predLength, SearchAndUpdateDriver.ATTR_MAX);
			
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
		return searchQuery;
	}
	
	
	private HashMap<String, Boolean> pickDistinctAttrs( int numAttrsToPick, Random randGen )
	{
		HashMap<String, Boolean> hashMap = new HashMap<String, Boolean>();
		int currAttrNum = 0;
		
		while(hashMap.size() != numAttrsToPick)
		//for(int i=0; i<numAttrsToPick; i++)
		{
			if( SearchAndUpdateDriver.numAttrs == SearchAndUpdateDriver.numAttrsInQuery )
			{
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
				currAttrNum++;
			}
			else
			{
				String attrName = SearchAndUpdateDriver.pickAttrUsingGaussian(randGen);
				hashMap.put(attrName, true);
			}
		}
		return hashMap;
	}
	
	
	private void sendALocMessage( long reqIdNum )
	{
//		String userGUID = SearchAndUpdateDriver.pickGUIDUsingGaussian(updateRand);
		
		int guidNum = generalRand.nextInt((int)SearchAndUpdateDriver.numUsers);
		
		String userGUID = SearchAndUpdateDriver.getSHA1(SearchAndUpdateDriver.guidPrefix+guidNum);
		
		String uAttrName = SearchAndUpdateDriver.pickAttrUsingGaussian(updateRand);
		
		double uAttrVal = SearchAndUpdateDriver.convertGuassianIntoValInRange
											(updateRand);
		
		
		JSONObject attrValJSON = new JSONObject();
		try
		{
			attrValJSON.put(uAttrName, uAttrVal);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		ExperimentUpdateReply updateRep = new ExperimentUpdateReply(reqIdNum, userGUID);
		
		SearchAndUpdateDriver.csClient.sendUpdateWithCallBack
										(userGUID, null, 
										attrValJSON, -1, updateRep, this.getCallBack());
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
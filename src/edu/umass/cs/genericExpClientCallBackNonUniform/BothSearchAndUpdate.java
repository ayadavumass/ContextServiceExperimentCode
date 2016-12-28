package edu.umass.cs.genericExpClientCallBackNonUniform;

import java.util.HashMap;
import java.util.Iterator;
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
		
		double avgResultSize = 0;
		if( this.numSearchesRecvd > 0 )
		{
			avgResultSize = (sumResultSize/this.numSearchesRecvd);
		}
		
		System.out.println("Both result:Goodput "+sysThrput+" average resultsize "
										+avgResultSize);
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
		String searchQuery = "";
		
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
			= pickDistinctAttrs( SearchAndUpdateDriver.numAttrsInQuery,
					SearchAndUpdateDriver.numAttrs, searchQueryRand );
		
		Iterator<String> attrIter = distinctAttrMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			double attrMin = SearchAndUpdateDriver.convertGuassianIntoValInRange
							(searchQueryRand.nextGaussian());
			
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
	
	
	private HashMap<String, Boolean> pickDistinctAttrs( int numAttrsToPick, 
			int totalAttrs, Random randGen )
	{
		HashMap<String, Boolean> hashMap = new HashMap<String, Boolean>();
		int currAttrNum = 0;
		
		while(hashMap.size() != numAttrsToPick)
		{
			if( SearchAndUpdateDriver.numAttrs == SearchAndUpdateDriver.numAttrsInQuery )
			{
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
				currAttrNum++;
			}
			else
			{
				String attrName = pickAttrUsingGaussian(randGen);
				hashMap.put(attrName, true);
			}
		}
		return hashMap;
	}
	
	private String pickAttrUsingGaussian(Random randGen)
	{
		// between -2 and 2.
		double gaussianRandVal = randGen.nextGaussian();
		double midPoint = SearchAndUpdateDriver.numAttrs/2.0;	
		
		if( gaussianRandVal >= 0 )
		{
			if( gaussianRandVal > 2 )
			{
				gaussianRandVal = 2;
			}
			
			int attrNum = (int) Math.ceil(midPoint + (gaussianRandVal*midPoint)/2.0);
			
			// because attr are numbered from 0 to NUM_ATTRs-1
	 		if( attrNum >= 1)
			{
				attrNum = attrNum -1 ;
			}
			
			String attrName = "attr"+attrNum;
			return attrName;
		}
		else
		{
			gaussianRandVal = -gaussianRandVal;
			
			if( gaussianRandVal > 2 )
			{
				gaussianRandVal = 2;
			}
			
			int attrNum = (int) Math.ceil((gaussianRandVal*midPoint)/2.0);
			
			// because attr are numbered from 0 to NUM_ATTRs-1
			if( attrNum >= 1)
			{
				attrNum = attrNum -1 ;	
			}
			
			String attrName = "attr"+attrNum;
			
			return attrName;
		}
	}
	
	
	private void sendALocMessage( int currUserGuidNum, long reqIdNum )
	{
		String userGUID = SearchAndUpdateDriver.getSHA1
				(SearchAndUpdateDriver.guidPrefix+currUserGuidNum);
		
		String uAttrName = pickAttrUsingGaussian(updateRand);
		
		double uAttrVal = SearchAndUpdateDriver.convertGuassianIntoValInRange
											(updateRand.nextGaussian());
		
		
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
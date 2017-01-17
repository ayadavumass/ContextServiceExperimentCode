package edu.umass.cs.ubercasestudy;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;
import edu.umass.cs.contextservice.messages.dataformat.SearchReplyGUIDRepresentationJSON;

public class ExperimentSearchReply implements SearchReplyInterface
{
	// used to demultiples when a reply comes back
	private final long requestNum;
	private JSONArray replyArray;
	private int replySize;
	private final long startTime;
	private  long finishTime;
	
	private final TaxiRideInfo taxiRideInfo;
	private final double searchRange;
	
	public ExperimentSearchReply( long requestNum, TaxiRideInfo taxiRideInfo, 
										double searchRange)
	{
		this.requestNum = requestNum;
		this.taxiRideInfo = taxiRideInfo;
		this.searchRange = searchRange;
		startTime = System.currentTimeMillis();
	}
	
	@Override
	public long getCallerReqId()
	{
		return requestNum;
	}
	
	@Override
	public void setSearchReplyArray(JSONArray csReplyArray)
	{
		this.replyArray = new JSONArray();
		for( int i=0; i<csReplyArray.length(); i++ )
		{
			try
			{
				JSONArray jsoArr1 = csReplyArray.getJSONArray(i);
				for(int j=0; j<jsoArr1.length(); j++)
				{
					JSONObject searchRepJSON = jsoArr1.getJSONObject(j);
					SearchReplyGUIDRepresentationJSON searchRepObj 
							= SearchReplyGUIDRepresentationJSON.fromJSONObject(searchRepJSON);
					replyArray.put(searchRepObj.getID());
				}
			} catch ( JSONException e )
			{
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void setReplySize(int replySize)
	{
		this.replySize = replySize;
	}
	
	@Override
	public int getReplySize()
	{
		return replySize;
	}
	
	@Override
	public JSONArray getSearchReplyArray()
	{
		return replyArray;
	}
	
	public void setCompletionTime()
	{
		finishTime = System.currentTimeMillis();
	}
	
	public long getCompletionTime()
	{
		return finishTime-startTime;
	}
	
	public TaxiRideInfo getTaxiRideInfo()
	{
		return taxiRideInfo;
	}
	
	public double getSearchRange()
	{
		return this.searchRange;
	}
	
	public boolean isFullRangeQuery()
	{
		
		double latMin 
				= Math.max(taxiRideInfo.getPickUpLat() - searchRange, Driver.MIN_LAT);
		double latMax 
				= Math.min(taxiRideInfo.getPickUpLat() + searchRange, Driver.MAX_LAT);
		
		double longMin 
				= Math.max(taxiRideInfo.getPickUpLong() - searchRange, Driver.MIN_LONG);
		double longMax 
				= Math.min(taxiRideInfo.getPickUpLong() + searchRange, Driver.MAX_LONG);
		
		
		boolean fullRangeQuery = false;
		
		if( (latMin == Driver.MIN_LAT) && (latMax == Driver.MAX_LAT) 
						&& (longMin == Driver.MIN_LONG) && (longMax == Driver.MAX_LONG) )
		{
			fullRangeQuery = true;
		}
		return fullRangeQuery;
	}
}
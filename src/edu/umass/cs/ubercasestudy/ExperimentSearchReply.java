package edu.umass.cs.ubercasestudy;

import org.json.JSONArray;

import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;

public class ExperimentSearchReply implements SearchReplyInterface
{
	// used to demultiples when a reply comes back
	private final long taxiReqID;
	private JSONArray replyArray;
	private int replySize;
	private final long startTime;
	private  long finishTime;
	
	public ExperimentSearchReply( long taxiReqID )
	{
		this.taxiReqID = taxiReqID;
		startTime = System.currentTimeMillis();
	}
	
	@Override
	public long getCallerReqId()
	{
		return taxiReqID;
	}
	
	@Override
	public void setSearchReplyArray(JSONArray replyArray)
	{
		this.replyArray = replyArray;
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
}
package edu.umass.cs.genericExpClientCallBack;

import org.json.JSONArray;

import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;

public class ExperimentSearchReply implements SearchReplyInterface
{
	private final long callerReqID;
	private JSONArray replyArray;
	private int replySize;
	private final long startTime;
	
	
	public ExperimentSearchReply( long callerReqID )
	{
		this.callerReqID = callerReqID;
		startTime = System.currentTimeMillis();
	}
	
	@Override
	public long getCallerReqId()
	{
		return callerReqID;
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
	
	public void printCompletionTime()
	{
		System.out.println("Search completion time reqID "+callerReqID
				+ " time taken "+(System.currentTimeMillis()-startTime)
				+" replySize "+replySize);
	}
}
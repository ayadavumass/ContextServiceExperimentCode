package edu.umass.cs.genericExpClientCallBack;

import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;

public class ExperimentUpdateReply implements UpdateReplyInterface
{
	private final long callerReqID;
	private final long startTime;
	
	public ExperimentUpdateReply( long callerReqID )
	{
		this.callerReqID = callerReqID;
		startTime = System.currentTimeMillis();
	}
	
	@Override
	public long getCallerReqId()
	{
		return callerReqID;
	}
	
	public void printCompletionTime()
	{
		System.out.println("Update completion time reqID "+callerReqID
				+ " time taken "+(System.currentTimeMillis()-startTime));
	}
}
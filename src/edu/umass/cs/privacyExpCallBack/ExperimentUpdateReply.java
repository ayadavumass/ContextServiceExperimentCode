package edu.umass.cs.privacyExpCallBack;

import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;

public class ExperimentUpdateReply implements UpdateReplyInterface
{
	private final long callerReqID;
	private final long startTime;
	private final String guidString;
	
	public ExperimentUpdateReply( long callerReqID, String Guid )
	{
		this.callerReqID = callerReqID;
		this.guidString  = Guid;
		startTime = System.currentTimeMillis();
	}
	
	@Override
	public long getCallerReqId()
	{
		return callerReqID;
	}
	
	public long getCompletionTime()
	{
		long timeTaken = (System.currentTimeMillis()-startTime);
		return timeTaken;
//		System.out.println("Update completion time reqID "+callerReqID
//				+ " time taken "+(System.currentTimeMillis()-startTime));
	}
	
	public String getGuid()
	{
		return this.guidString;
	}
}
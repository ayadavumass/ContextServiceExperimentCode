package edu.umass.cs.genericExpClientCallBack;

import edu.umass.cs.contextservice.client.callback.interfaces.CallBackInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;

public class ExperimentCallBack implements CallBackInterface
{
	// key is the privacy req requesID,  value is the 
	// anonymized ID update tracker.
	// key is req ID, value is the start time
	//private ConcurrentHashMap<Long, Long> requestIDMap;
	private final AbstractRequestSendingClass reqSendClass;
	
	public ExperimentCallBack(AbstractRequestSendingClass reqSendClass)
	{
		this.reqSendClass = reqSendClass;
	}
	
	@Override
	public void searchCompletion(SearchReplyInterface searchRep)
	{
		((ExperimentSearchReply)searchRep).printCompletionTime();
		reqSendClass.incrementSearchNumRecvd(searchRep.getReplySize(), -1);
	}
	
	@Override
	public void updateCompletion(UpdateReplyInterface updateRep)
	{
		((ExperimentSearchReply)updateRep).printCompletionTime();
		reqSendClass.incrementUpdateNumRecvd("", -1);
	}
}
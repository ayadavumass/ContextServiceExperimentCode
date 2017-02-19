package edu.umass.cs.largescalecasestudy;

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
		long timeTaken = ((ExperimentSearchReply)searchRep).getCompletionTime();
		reqSendClass.incrementSearchNumRecvd(searchRep.getReplySize(), timeTaken);
	}
	
	@Override
	public void updateCompletion(UpdateReplyInterface updateRep)
	{
		long timeTaken = ((ExperimentUpdateReply)updateRep).getCompletionTime();
		String guid = ((ExperimentUpdateReply)updateRep).getGuid();
		reqSendClass.incrementUpdateNumRecvd(guid, timeTaken);
	}
}
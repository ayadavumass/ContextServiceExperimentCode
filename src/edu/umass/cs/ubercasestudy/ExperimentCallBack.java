package edu.umass.cs.ubercasestudy;

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
	
	
	public void searchCompletion(SearchReplyInterface searchRep)
	{
		//long timeTaken = ((ExperimentSearchReply)searchRep).getCompletionTime();
		//reqSendClass.incrementSearchNumRecvd((ExperimentSearchReply)searchRep);
		Driver.execServ.execute(new SearchReplyThread((ExperimentSearchReply)searchRep));
	}
	
	
	public void updateCompletion(UpdateReplyInterface updateRep)
	{
//		long timeTaken = ((ExperimentUpdateReply)updateRep).getCompletionTime();
//		String guid = ((ExperimentUpdateReply)updateRep).getGuid();
		reqSendClass.incrementUpdateNumRecvd((ExperimentUpdateReply)updateRep);
	}
	
	
	public class SearchReplyThread implements Runnable
	{
		ExperimentSearchReply searchRep;
		public SearchReplyThread(ExperimentSearchReply searchRep)
		{
			this.searchRep = searchRep;
		}
		@Override
		public void run() 
		{
			reqSendClass.incrementSearchNumRecvd(searchRep);
		}
	}
	
	public class UpdateReplyThread implements Runnable
	{
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			
		}	
	}
}
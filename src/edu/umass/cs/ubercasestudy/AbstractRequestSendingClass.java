package edu.umass.cs.ubercasestudy;

import java.util.Timer;
import java.util.TimerTask;

import edu.umass.cs.contextservice.client.callback.interfaces.CallBackInterface;


public abstract class AbstractRequestSendingClass
{
	protected long expStartTime;
	protected final Timer waitTimer;
	protected final Object waitLock = new Object();
	protected long numSent;
	protected long numRecvd;
	protected boolean threadFinished;
	protected final Object threadFinishLock = new Object();
	
	// 1% loss tolerance
	private final double lossTolerance;
	
	private final CallBackInterface expCallback;
	private final long waitTime;
	
	public AbstractRequestSendingClass( double lossTolerance, long waitTime )
	{
		expCallback =  new ExperimentCallBack(this);
		threadFinished = false;
		this.lossTolerance = lossTolerance;
		this.waitTime = waitTime;
		numSent = 0;
		numRecvd = 0;
		waitTimer = new Timer();
	}
	
	protected void startExpTime()
	{
		expStartTime = System.currentTimeMillis();
	}
	
	protected void waitForFinish()
	{
		waitTimer.schedule(new WaitTimerTask(), waitTime);
		
		synchronized(waitLock)
		{
			while( !checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				try
				{
					waitLock.wait();
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
		
		//stopThis();	
		waitTimer.cancel();
		
		threadFinished = true;
		synchronized( threadFinishLock )
		{
			threadFinishLock.notify();
		}
		//System.exit(0);
	}
	
	public void waitForThreadFinish()
	{
		synchronized( threadFinishLock )
		{
			while( !threadFinished )
			{
				try 
				{
					threadFinishLock.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public class WaitTimerTask extends TimerTask
	{			
		@Override
		public void run()
		{
			// print the remaining update and query times
			// and finish the process, cancel the timer and exit JVM.
			//stopThis();
			
			double endTimeReplyRecvd = System.currentTimeMillis();
			double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
			
			System.out.println(this.getClass().getName()+" Result:TimeOutThroughput "+sysThrput);
			
			waitTimer.cancel();
			// just terminate the JVM
			//System.exit(0);
			threadFinished = true;
			synchronized( threadFinishLock )
			{
				threadFinishLock.notify();
			}
			System.exit(0);
		}
	}
	
	
	public CallBackInterface getCallBack()
	{
		return this.expCallback;
	}
	
	protected boolean checkForCompletionWithLossTolerance
											(double numSent, double numRecvd)
	{
		boolean completion = false;
		
		double withinLoss = (lossTolerance * numSent)/100.0;
		if( (numSent - numRecvd) <= withinLoss )
		{
			completion = true;
		}
		return completion;
	}
	
	public abstract void incrementUpdateNumRecvd(ExperimentUpdateReply expUpdateReply);
	public abstract void incrementSearchNumRecvd(ExperimentSearchReply expSearchReply);
}
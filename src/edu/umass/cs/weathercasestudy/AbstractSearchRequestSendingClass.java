package edu.umass.cs.weathercasestudy;

import java.util.Timer;
import java.util.TimerTask;

import edu.umass.cs.contextservice.client.callback.interfaces.CallBackInterface;


public abstract class AbstractSearchRequestSendingClass
{
	protected static long expStartTime;
	protected static final Timer waitTimer = new Timer();;
	protected static final Object waitLock = new Object();
	protected static long numSent;
	protected static long numRecvd;
	protected static boolean threadFinished;
	protected static final Object threadFinishLock = new Object();
	
	// 1% loss tolerance
	private static final double lossTolerance = SearchAndUpdateDriver.SEARCH_LOSS_TOLERANCE;
	
	private final CallBackInterface expCallback;
	private static final long waitTime = SearchAndUpdateDriver.WAIT_TIME;
	
	public AbstractSearchRequestSendingClass( )
	{
		expCallback =  new ExperimentCallBack(null, this);
		threadFinished = false;
		numSent = 0;
		numRecvd = 0;
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
	
	public abstract void incrementUpdateNumRecvd(String userGUID, long timeTaken);
	public abstract void incrementSearchNumRecvd(int resultSize, long timeTaken);
}
package edu.umass.cs.niobenchmarking;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.common.ContextServiceDemultiplexer;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.NoopMessage;
import edu.umass.cs.contextservice.messages.NoopReplyMessage;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;

public class NIOThroughputBenchmarking implements PacketDemultiplexer<JSONObject>
{
	public static final int EXPERIMENT_TIME						= 100000;
	public static final int WAIT_TIME							= 100000;
	
	public static final int SENDER								= 1;
	public static final int RECIEVER							= 2;
	
	private final CSNodeConfig<Integer> clientNodeConfig;
	private final JSONNIOTransport<Integer> niot;
	private final JSONMessenger<Integer> messenger;
	private final String sourceIP;
	private final int sourcePort;
	
	private final String destIpAddress;
	private final int destPort;
	
	private long numSent 										= 0;
	private long numRecvd										= 0;
	
	private final Object lock 									= new Object();
	private final Timer waitTimer;
	private final long expStartTime;
	private static double reqsps;
	private static int payloadLen;
	private static String payloadStr;
	
	public NIOThroughputBenchmarking( String sourceIPAddress, int sourcePort, 
			 String destIPAddress, int destPort) throws IOException
	{
		this.sourceIP = sourceIPAddress;
		this.sourcePort =  sourcePort;
		this.destIpAddress = destIPAddress;
		this.destPort = destPort;
		waitTimer = new Timer();
		
		clientNodeConfig =  new CSNodeConfig<Integer>();
		int id = 0;
		clientNodeConfig.add(id, new InetSocketAddress(sourceIP, sourcePort));
        
        AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		
        //System.out.println("\n\n node IP "+localNodeConfig.getNodeAddress(myID) +
		//		" node Port "+localNodeConfig.getNodePort(myID)+" nodeID "+myID);
		
		niot = new JSONNIOTransport<Integer>(id,  clientNodeConfig, pd , true);
		
		messenger = 
			new JSONMessenger<Integer>(niot);
		
		pd.register(ContextServicePacket.PacketType.NOOP_MEESAGE, this);
		pd.register(ContextServicePacket.PacketType.NOOP_REPLY_MESSAGE, this);
		
		messenger.addPacketDemultiplexer(pd);
		
		expStartTime = System.currentTimeMillis();
	}

	@Override
	public boolean handleMessage(JSONObject message, NIOHeader nioHeader) 
	{
		try
		{
			if( message.getInt(ContextServicePacket.PACKET_TYPE) 
					== ContextServicePacket.PacketType.NOOP_REPLY_MESSAGE.getInt() )
			{
				synchronized(lock)
				{
					numRecvd++;
					if(checkForCompletionWithLossTolerance(numSent, numRecvd))
					{
						lock.notify();
					}
				}
			}else if( message.getInt(ContextServicePacket.PACKET_TYPE) 
					== ContextServicePacket.PacketType.NOOP_MEESAGE.getInt() )
			{
				NoopMessage<Integer> noopMesg = new NoopMessage<Integer>(message);
				String sourceIP = noopMesg.getSourceIP();
				int sourcePort = noopMesg.getSourcePort();
				NoopReplyMessage<Integer> noopRepMesg = new NoopReplyMessage<Integer>(0);
				
				try 
				{
					messenger.sendToAddress(new InetSocketAddress(sourceIP, sourcePort),
							noopRepMesg.toJSONObject() );
				} catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
		catch(JSONException jsonEx)
		{
			jsonEx.printStackTrace();
		}
		return true;
	}
	
	public void rateControlledRequestSender() throws Exception
	{
		// as it is per ms
		double reqspms = reqsps/1000.0;
		long currTime  = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		payloadStr = "";
		for(int i=0;i<payloadLen; i++)
		{
			payloadStr = payloadStr + 'a';
		}
		
		while( ( (System.currentTimeMillis() - expStartTime)
				< EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendNoopMessage();
				numSent++;
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0);
			double numberShouldBeSentByNow = timeElapsed*reqspms;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
				sendNoopMessage();
				numSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("NIOThrouhgput eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("NIOThrouhgput result:Goodput "+sysThrput);
	}
	
	private void waitForFinish()
	{
		waitTimer.schedule(new WaitTimerTask(), WAIT_TIME);
		
		synchronized(lock)
		{
			while( !checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				try
				{
					lock.wait();
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
		waitTimer.cancel();
	}
	
	private boolean checkForCompletionWithLossTolerance(double numSent, double numRecvd)
	{
		boolean completion = false;
		
		double withinLoss = (0.0 * numSent)/100.0;
		if( (numSent - numRecvd) <= withinLoss )
		{
			completion = true;
		}
		return completion;
	}
	
	private void sendNoopMessage()
	{
		try {
			NoopMessage<Integer> noopMesg = new NoopMessage<Integer>(0, sourceIP, sourcePort, 
					payloadStr);
			messenger.sendToAddress(new InetSocketAddress(destIpAddress, destPort), 
					noopMesg.toJSONObject());
		} catch (IOException | JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			System.exit(0);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		reqsps = Double.parseDouble(args[0]);
		String sourceIP = args[1];
		int sourcePort = Integer.parseInt(args[2]);
		String destIP = args[3];
		int destPort = Integer.parseInt(args[4]);
		int type = Integer.parseInt(args[5]);
		payloadLen = Integer.parseInt(args[6]);
		
		NIOThroughputBenchmarking nioTester 
				= new NIOThroughputBenchmarking(sourceIP, sourcePort, destIP, destPort);
		
		if(type == SENDER)
		{
			nioTester.rateControlledRequestSender();
		}
	}
}
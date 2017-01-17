package edu.umass.cs.ubercasestudy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class TaxiQueryIssue extends AbstractRequestSendingClass
{
	private double minLat						= 8000;
	private double maxLat						= -8000;
	
	private double minLong						= 8000;
	private double maxLong						= -8000;
	
	private final Random randGen;
	
	private final PriorityBlockingQueue<TaxiRideInfo> ongoingTaxiRides;
	
	private long requestNum						= 0;
	
	//private final Object lockObj 				= new Object();
	private long sumSearchTime					= 0;
	private long sumSearchReply					= 0;
	private long sumUpdateTime					= 0;
	
	private long numSearch						= 0;
	private long numUpdate						= 0;
	
	private long noTaxiFound					= 0;
	
    //new PriorityQueue<String>(10, comparator);
	public TaxiQueryIssue()
	{
		super(Driver.LOSS_TOLERANCE, (long) Driver.WAIT_TIME);
		randGen = new Random();
		Comparator<TaxiRideInfo> comparator = new TaxiRideInfo(-1, -1, -1, 
																	-1, -1);
		ongoingTaxiRides = new PriorityBlockingQueue<TaxiRideInfo>(10,comparator );
	}
	
	// this function will be called from a thread.
	public void startIssuingQueries()
	{
		long startTime = System.currentTimeMillis();
		
		DropOffThread dpt = new DropOffThread();
		new Thread(dpt).start();
		
		
		BufferedReader br = null;
		
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader(Driver.ONE_DAY_TRACE_PATH));
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				//System.out.println("line from file "+sCurrentLine);
				if(sCurrentLine.startsWith("#"))
					continue;
				
				double random = randGen.nextDouble();
				
				if(!(random <= Driver.REQUEST_ISSUE_PROB))
					continue;
				
				try 
				{
					String[] lineParsed = sCurrentLine.split(",");
					String pickDateTimeString = lineParsed[Driver.PICKUP_DATETIME_INDEX-1];
					String dropDateTimeString = lineParsed[Driver.DROPOFF_DATETIME_INDEX-1];
					
					long pickupTime = Driver.dfm.parse(pickDateTimeString).getTime()/1000;
					long dropOffTime = Driver.dfm.parse(dropDateTimeString).getTime()/1000;
					
					synchronized(Driver.TIME_WAIT_LOCK)
					{
						while(pickupTime > Driver.getCurrUnixTime())
						{
							try 
							{
								Driver.TIME_WAIT_LOCK.wait();
							} catch (InterruptedException e) 
							{
								e.printStackTrace();
							}
						}
					}
					// now issue the query.
					
					//System.out.println("Sending query "+pickDateTimeString
					//		+" "+dropDateTimeString);
					
					double pickupLat  
							= Double.parseDouble(lineParsed[Driver.PICKUP_LAT_INDEX-1]);
					double pickupLong 
							= Double.parseDouble(lineParsed[Driver.PICKUP_LONG_INDEX-1]);
					
					double dropOffLat  
							= Double.parseDouble(lineParsed[Driver.DROPOFF_LAT_INDEX-1]);
					double dropOffLong 
							= Double.parseDouble(lineParsed[Driver.DROPOFF_LONG_INDEX-1]);
					
					if( pickupLat == 0.0 || pickupLong == 0.0 || dropOffLat == 0.0 || 
							dropOffLong == 0.0 )
					{
						System.out.println("lineParsed");
					}

					sendOutTaxiRequest(pickupLat, pickupLong, 
							dropOffLat, dropOffLong, requestNum++, dropOffTime, 
							Driver.SEARCH_AREA_RANGE);
					
				}
				catch (ParseException e) 
				{
					e.printStackTrace();
				}
			}
		} 
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if ( br != null )
					br.close();
			} catch ( IOException ex )
			{
				ex.printStackTrace();
			}
		}
		
		this.waitForFinish();
		
		if(numSearch > 0)
		{
			System.out.println("Average search time "+(this.sumSearchTime/numSearch)
					+" numSearchReqs "+numSearch+" SearchReplySize "
					+this.sumSearchReply/numSearch +" noTaxiFoundFullSearch "+noTaxiFound);
		}
		
		if(numUpdate > 0)
		{
			System.out.println("Average update time "+(this.sumUpdateTime/numUpdate)
					+" numUpdateReqs "+numUpdate);
		}
		double thpt = (numSent*1000.0)/(System.currentTimeMillis()- startTime);
		if(this.timeout)
			System.out.println("TimeOut: Goodput="+thpt+" numSent="+numSent+" numRecvd="+numRecvd);
		else
			System.out.println("Successfull: Goodput="+thpt+" numSent="+numSent+" numRecvd="+numRecvd);
	}
	
	// this function will also be called from a thread.
	public void checkFinishedTaxiRequests()
	{
		while(true)
		{
			TaxiRideInfo taxiRideInfo = null;
			synchronized(this.ongoingTaxiRides)
			{
				taxiRideInfo = this.ongoingTaxiRides.peek();
			}
			
			if(taxiRideInfo != null)
			{
				long dropOffTime = taxiRideInfo.getRideEndTimeStamp();
				
				synchronized(Driver.TIME_WAIT_LOCK)
				{
					while(dropOffTime > Driver.getCurrUnixTime())
					{
						try 
						{
							Driver.TIME_WAIT_LOCK.wait();
						} catch (InterruptedException e) 
						{
							e.printStackTrace();
						}
					}
				}
				
				// drop off time
				synchronized(this.ongoingTaxiRides)
				{
					taxiRideInfo = this.ongoingTaxiRides.poll();
				}
				
				String taxiGUID = taxiRideInfo.getTaxiGuid();
				double dropOffLat = taxiRideInfo.getDropOffLat();
				double dropOffLong = taxiRideInfo.getDropOffLong();
				
				double freeRand = randGen.nextDouble()* Driver.FREE_INUSE_BOUNDARY;
				updateTaxiGUID(taxiGUID, dropOffLat, dropOffLong, freeRand);
				
				synchronized(Driver.taxiFreeMap)
				{
					Driver.taxiFreeMap.put(taxiGUID, true);
				}
			}
			else
			{
				synchronized(ongoingTaxiRides)
				{
					while(ongoingTaxiRides.size() == 0)
					{
						try 
						{
							ongoingTaxiRides.wait();
						} catch (InterruptedException e) 
						{
							e.printStackTrace();
						}
					}
				}
			}
			
		}
	}
	
	
	private boolean sendOutTaxiRequest(double pickupLat, double pickupLong, 
						double dropOffLat, double dropOffLong, long currReqNum, 
						long dropOffTime, double searchRange)
	{
		double latMin = Math.max(pickupLat - searchRange, Driver.MIN_LAT);
		double latMax = Math.min(pickupLat + searchRange, Driver.MAX_LAT);
		
		double longMin = Math.max(pickupLong - searchRange, Driver.MIN_LONG);
		double longMax = Math.min(pickupLong + searchRange, Driver.MAX_LONG);
		
		String searchQuery = Driver.LAT_ATTR +" >= "+latMin
				+" AND "+Driver.LAT_ATTR+" <= "+latMax
				+" AND "+Driver.LONG_ATTR +" >= "+longMin
				+" AND "+Driver.LONG_ATTR+" <= "+longMax
				+" AND "+Driver.STATUS_ATTR+" >= "+Driver.MIN_STATUS
				+" AND "+Driver.STATUS_ATTR+" <= "+Driver.FREE_INUSE_BOUNDARY;
		
		TaxiRideInfo taxiRideInfo = new TaxiRideInfo(dropOffTime, pickupLat, 
				pickupLong, dropOffLat, dropOffLong);
		
//		TaxiRideInfo taxiRideInfo = new TaxiRideInfo();
//		long taxiRideEndTimeStamp, double pickUpLat, 
//		double pickUpLong, double dropOffLat, double dropOffLong
		
		ExperimentSearchReply searchRep 
				= new ExperimentSearchReply(currReqNum, taxiRideInfo, searchRange);
		
		synchronized(waitLock)
		{
			numSent++;
			numSearch++;
		}
		
		Driver.csClient.sendSearchQueryWithCallBack
						(searchQuery, 300000, searchRep, this.getCallBack());
		
		boolean fullRangeQuery = false;
		
		if( (latMin == Driver.MIN_LAT) && (latMax == Driver.MAX_LAT) 
						&& (longMin == Driver.MIN_LONG) && (longMax == Driver.MAX_LONG) )
		{
			fullRangeQuery = true;
		}
		return fullRangeQuery;
	}
	
	
	public static void main( String[] args )
	{
		TaxiQueryIssue taxObj = new TaxiQueryIssue();
		taxObj.computeLatLongBounds();
	}


	@Override
	public void incrementUpdateNumRecvd(ExperimentUpdateReply expUpdateReply) 
	{
		synchronized(waitLock)
		{
			this.sumUpdateTime = this.sumUpdateTime + expUpdateReply.getCompletionTime();
			
			numRecvd++;
			
			//System.out.println("Update recvd numSent "+ numSent+" numRecvd "+numRecvd
			//		 +" numUpdate "+numUpdate);
			if( checkForCompletionWithLossTolerance() )
			{
				waitLock.notify();
			}
		}
	}

	@Override
	public void incrementSearchNumRecvd(ExperimentSearchReply expSearchReply)
	{	
		expSearchReply.setCompletionTime();
		
		TaxiRideInfo taxiRideInfo = expSearchReply.getTaxiRideInfo();
		// taxi reply has come back. so choose randomly one taxi 
		// and update its GUID and location.
		
		JSONArray taxiGUIDArray = expSearchReply.getSearchReplyArray();
		//System.out.println("taxiGUIDArray len "+taxiGUIDArray.length());
		
		assert(taxiGUIDArray != null);
		
		String taxiGUID = "";
		
		if(taxiGUIDArray.length() > 0)
		{
			taxiGUID = pickAFreeTaxi(taxiGUIDArray);
			
			//System.out.println("taxiGUID "+taxiGUID);
			
			if(taxiGUID.length() > 0)
			{
				instantiateTaxiRide(taxiRideInfo, taxiGUID);
			}
		}
		
		boolean noTaxiFoundFullSearch = false;
		if(taxiGUID.length()<= 0)
		{
			// send query again with bigger search range
			if(expSearchReply.isFullRangeQuery())
			{
				//System.out.println("No taxi found even for full range query");
				noTaxiFoundFullSearch = true;
			}
			else
			{
				double currSearchRange = expSearchReply.getSearchRange();
				currSearchRange = currSearchRange + Driver.SEARCH_AREA_RANGE;
				sendOutTaxiRequest(taxiRideInfo.getPickUpLat(), taxiRideInfo.getPickUpLong(), 
						taxiRideInfo.getDropOffLat(), taxiRideInfo.getDropOffLong(),
						expSearchReply.getCallerReqId(), 
						taxiRideInfo.getRideEndTimeStamp(), currSearchRange);
			}	
		}
		
		synchronized(waitLock)
		{
			this.sumSearchTime = this.sumSearchTime + expSearchReply.getCompletionTime();
			this.sumSearchReply = this.sumSearchReply + taxiGUIDArray.length();
			
			if(noTaxiFoundFullSearch)
			{
				this.noTaxiFound++;
			}
			
			numRecvd++;
			//System.out.println("Search recvd numSent "+ numSent+" numRecvd "+numRecvd
			//		 +" numSearch "+numSearch);
			
			if( checkForCompletionWithLossTolerance() )
			{
				waitLock.notify();
			}
		}
	}
	
	private String pickAFreeTaxi(JSONArray resultTaxiArray)
	{
		String taxiGUID = "";
		
		for(int i=0; i<resultTaxiArray.length(); i++)
		{
			try 
			{
				String currGUID = resultTaxiArray.getString(i);
				
				synchronized(Driver.taxiFreeMap)
				{
					//System.out.println("currGUID "+currGUID);
					// taxi is free
					if(Driver.taxiFreeMap.get(currGUID))
					{
						// taxi busy.
						Driver.taxiFreeMap.put(currGUID, false);
						taxiGUID = currGUID;
						break;
					}
				}
			} 
			catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		return taxiGUID;
	}
	
	
	private void instantiateTaxiRide(TaxiRideInfo taxiRideInfo, String taxiGUID)
	{
		// set taxi in taxiRideInfo
		taxiRideInfo.setTaxiGUID(taxiGUID);
		double pickUpLat = taxiRideInfo.getPickUpLat();
		double pickUpLong = taxiRideInfo.getPickUpLong();
		
		double inUseRand = Driver.MAX_STATUS- 
						(randGen.nextDouble()* Driver.FREE_INUSE_BOUNDARY);
		// update location
		updateTaxiGUID(taxiGUID, pickUpLat, 
				pickUpLong, inUseRand);		
		
		assert(taxiRideInfo != null);
		
		//System.out.println("ongoingTaxiRides size "+ongoingTaxiRides.size());
		
		// put taxiRideInfo in drop-off priority queue
		// taxi ride has started, the taxi ride will be removed
		// from this queue at the drop off time.
		
		synchronized(ongoingTaxiRides)
		{
			this.ongoingTaxiRides.add(taxiRideInfo);
			ongoingTaxiRides.notify();
		}
	}
	
	
	private void updateTaxiGUID(String taxiGUID, double latitude, 
			double longitude, double status)
	{
		JSONObject attrValJSON = new JSONObject();			
			
		try
		{
			attrValJSON.put( Driver.LAT_ATTR, latitude );
			attrValJSON.put( Driver.LONG_ATTR, longitude );
			attrValJSON.put( Driver.STATUS_ATTR, status );
			
			ExperimentUpdateReply updateRep = new ExperimentUpdateReply(requestNum++, taxiGUID);
			
			synchronized(waitLock)
			{
				numSent++;
				numUpdate++;
			}
			Driver.csClient.sendUpdateWithCallBack
								( taxiGUID, null, 
								attrValJSON, -1, updateRep, this.getCallBack() );	
		} catch (JSONException e) 
		{
			e.printStackTrace();
		}
	}
	
	private class DropOffThread implements Runnable
	{
		@Override
		public void run() 
		{
			checkFinishedTaxiRequests();
		}
	}
	
	
	public void computeLatLongBounds()
	{
		BufferedReader br = null;
		
		try
		{
			String sCurrentLine;
			
			br = new BufferedReader(new FileReader
									(Driver.ONE_DAY_TRACE_PATH));
			
			while( (sCurrentLine = br.readLine()) != null )
			{
				if(sCurrentLine.startsWith("#"))
					continue;
				
				String[] lineParsed = sCurrentLine.split(",");
				//String dateTime = lineParsed[1];
				
				//String[] dateParsed = dateTime.split(" ");
				//String date = dateParsed[0];
				
				double pickupLat  
						= Double.parseDouble(lineParsed[Driver.PICKUP_LAT_INDEX-1]);
				double pickupLong 
						= Double.parseDouble(lineParsed[Driver.PICKUP_LONG_INDEX-1]);
				
				double dropOffLat  
						= Double.parseDouble(lineParsed[Driver.DROPOFF_LAT_INDEX-1]);
				double dropOffLong 
						= Double.parseDouble(lineParsed[Driver.DROPOFF_LONG_INDEX-1]);
				
				if( pickupLat == 0.0 || pickupLong == 0.0 || dropOffLat == 0.0 || 
						dropOffLong == 0.0 )
				{
					System.out.println("lineParsed");
				}
					
				
				if( pickupLat < minLat  )
				{
					minLat = pickupLat;
				}
				
				if( dropOffLat < minLat )
				{
					minLat = dropOffLat;
				}
				
				if( pickupLong < minLong  )
				{
					minLong = pickupLong;
				}
				
				if( dropOffLong < minLong )
				{
					minLong = dropOffLong;
				}
				
				
				if( pickupLat > maxLat  )
				{
					maxLat = pickupLat;
				}
				
				if( dropOffLat > maxLat )
				{
					maxLat = dropOffLat;
				}
				
				if( pickupLong > maxLong  )
				{
					maxLong = pickupLong;
				}
				
				if( dropOffLong > maxLong )
				{
					maxLong = dropOffLong;
				}
			}
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		finally
		{
			try
			{
				if ( br != null )
					br.close();
				
			} catch ( IOException ex )
			{
				ex.printStackTrace();
			}
		}
		
		System.out.println("minLat "+minLat+" maxLat "+maxLat
				+ " minLong "+minLong+" maxLong "+maxLong);
	}
	
//	@Override
//	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
//	{
//	}
}
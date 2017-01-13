package edu.umass.cs.ubercasestudy;

import java.util.Comparator;

public class TaxiRideInfo implements Comparator<TaxiRideInfo>
{
	private String taxiGUID;
	private final long taxiRideEndTimeStamp;
	private final double pickUpLat;
	private final double pickUpLong;
	private final double dropOffLat;
	private final double dropOffLong;
	
	public TaxiRideInfo(long taxiRideEndTimeStamp, double pickUpLat, 
			double pickUpLong, double dropOffLat, double dropOffLong)
	{
		this.taxiRideEndTimeStamp = taxiRideEndTimeStamp;
		this.pickUpLat = pickUpLat;
		this.pickUpLong = pickUpLong;
		this.dropOffLat = dropOffLat;
		this.dropOffLong = dropOffLong;
	}
	
	@Override
	public int compare(TaxiRideInfo o1, TaxiRideInfo o2) 
	{
		if(o1.taxiRideEndTimeStamp < o2.taxiRideEndTimeStamp)
		{
			return -1;
		}
		else if(o1.taxiRideEndTimeStamp > o2.taxiRideEndTimeStamp)
		{
			return 1;
		}
		else
		{
			return 0;
		}
	}
	
	
	public String getTaxiGuid()
	{
		return taxiGUID;
	}

	public double getDropOffLat()
	{
		return this.dropOffLat;
	}
	
	public double getPickUpLat()
	{
		return this.pickUpLat;
	}
	
	public double getPickUpLong()
	{
		return this.pickUpLong;
	}
	
	public double getDropOffLong()
	{
		return this.dropOffLong;
	}
	
	public long getRideEndTimeStamp()
	{
		return taxiRideEndTimeStamp;
	}
}
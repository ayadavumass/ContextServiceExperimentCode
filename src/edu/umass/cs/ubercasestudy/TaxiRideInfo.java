package edu.umass.cs.ubercasestudy;

import java.util.Comparator;

public class TaxiRideInfo implements Comparator<TaxiRideInfo>
{
	private final String taxiGUID;
	private final long taxiRideEndTimeStamp;
	
	public TaxiRideInfo(String taxiGUID, long taxiRideEndTimeStamp)
	{
		this.taxiGUID = taxiGUID;
		this.taxiRideEndTimeStamp = taxiRideEndTimeStamp;
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
	
	public long getRideEndTimeStamp()
	{
		return taxiRideEndTimeStamp;
	}
}
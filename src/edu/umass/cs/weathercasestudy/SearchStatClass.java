package edu.umass.cs.weathercasestudy;

public class SearchStatClass 
{
	double sendingRate;
	double systemThpt;
	double numSent;
	double numRecvd;
	double avgReplySize;
	double latency;
	double numOriginalSearch;
	double avgLatRange;
	double avgLongRange;
	
	public String toString()
	{
		String str = " numSent "+numSent
				+" numRecvd "+ numRecvd
				+" avg reply size "+ avgReplySize
				+ " sending rate "+sendingRate
				+ " system throughput "+systemThpt
				+ " latency "+latency+" ms "
				+ " numOriginalSearch "+numOriginalSearch
				+ " average LatRange "+avgLatRange
				+ " average LongRange "+avgLongRange;
		return str;
	}
}
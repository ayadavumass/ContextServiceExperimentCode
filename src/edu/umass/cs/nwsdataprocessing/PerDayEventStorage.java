package edu.umass.cs.nwsdataprocessing;

import java.util.Comparator;

public class PerDayEventStorage implements Comparator<PerDayEventStorage>
{
	public long timeslot;
	public long numEvents;
	public long totalPolygonsInEvents;
	
	
	@Override
	public int compare(PerDayEventStorage o1, PerDayEventStorage o2) 
	{
		if(o1.totalPolygonsInEvents < o2.totalPolygonsInEvents)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
}
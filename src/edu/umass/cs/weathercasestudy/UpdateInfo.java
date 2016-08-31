package edu.umass.cs.weathercasestudy;

public class UpdateInfo 
{
	private final int realId;
	private final TrajectoryEntry trajEntry;
	
	public UpdateInfo( int realId, TrajectoryEntry trajEntry )
	{
		this.realId = realId;
		this.trajEntry = trajEntry;
	}
	
	public int getRealId()
	{
		return this.realId;
	}
	
	public TrajectoryEntry getTrajEntry()
	{
		return this.trajEntry;
	}
}
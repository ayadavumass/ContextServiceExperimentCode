package edu.umass.cs.weathercasestudy;

public class ActiveQueryStorage
{
	// key used in map.
	private final String searchQueryKey;
	private final long issueUnixTime;
	private final long expiryUnixTime;
	
	private final String queryString;
	
	// last sent is in real time
	private long lastSentUnixTime;
	
	public ActiveQueryStorage( String searchQueryKey, long issueUnixTime, 
						long expiryUnixTime, String queryString )
	{
		this.searchQueryKey = searchQueryKey;
		this.issueUnixTime = issueUnixTime;
		this.expiryUnixTime = expiryUnixTime;
		this.queryString = queryString;
		
		lastSentUnixTime = issueUnixTime;
	}
	
	public String getSearchQueryKey()
	{
		return this.searchQueryKey;
	}
	
	public long getIssueUnixTime()
	{
		return issueUnixTime;
	}
	
	public long getExpiryUnixTime()
	{
		return this.expiryUnixTime;
	}
	
	public String getQueryString()
	{
		return this.queryString;
	}
	
	public void updateLastSentUnixTime(long newLastSentUnixTime)
	{
		this.lastSentUnixTime = newLastSentUnixTime;
	}
	
	public long getLastSentUnixTime()
	{
		return this.lastSentUnixTime;
	}
}
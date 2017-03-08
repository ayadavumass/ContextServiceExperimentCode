package edu.umass.cs.largescalecasestudy;

public class UserRecordInfo
{
	public static final int GUID_INDEX 				= 0;
	public static final int FILENAME_INDEX 			= 1;
	public static final int HOMELAT_INDEX 			= 2;
	public static final int HOMELONG_INDEX 			= 3;
	public static final int TOTALUPDS_INDEX 		= 4;
	public static final int NEXTUPDNUM_INDEX 		= 5;
	public static final int NEXTUPDTIME_INDEX		= 6;
	public static final int NEXTLAT_INDEX 			= 7;
	public static final int NEXTLONG_INDEX 			= 8;
	
	
	private String userGUID;
	private String filename;   // filename of the original user from the log.
	private double homeLoclLat;     // home location lat.
	private double homeLocLong;     // home location long.
	private int totalUpdates;  // for the current day.
	private int nextUpdateNum; // next update num to be performed.
	private long nextUpdateUnixTime;  // unix time in sec of the next update to be performed.
	// the above time is relative to the midnight of the current day in the timer thread time.
	private double nextUpdateLat;   // update lat of the next update
	private double nextUpdateLong;  // update long of the next update
	
	
	public UserRecordInfo( String userGUID, String filename, 
			double homeLoclLat, double homeLocLong, int totalUpdates,
			int nextUpdateNum, long nextUpdateUnixTime, 
			double nextUpdateLat, double nextUpdateLong )
	{
		this.userGUID = userGUID;
		this.filename = filename;
		this.homeLoclLat = homeLoclLat;
		this.homeLocLong = homeLocLong;
		this.totalUpdates = totalUpdates;
		this.nextUpdateNum = nextUpdateNum;
		this.nextUpdateUnixTime = nextUpdateUnixTime;
		this.nextUpdateLat = nextUpdateLat;
		this.nextUpdateLong = nextUpdateLong;
	}
	
	
	public static UserRecordInfo fromString(String strForm)
	{
		String[] parsed   = strForm.split(",");
		String userGUID   = parsed[GUID_INDEX];
		String filename   = parsed[FILENAME_INDEX];
		double homeLoclLat = Double.parseDouble(parsed[HOMELAT_INDEX]);
		double homeLocLong = Double.parseDouble(parsed[HOMELONG_INDEX]);
		int totalUpdates  = Integer.parseInt(parsed[TOTALUPDS_INDEX]);
		int nextUpdateNum = Integer.parseInt(parsed[NEXTUPDNUM_INDEX]);
		long nextUpdateUnixTime = Long.parseLong(parsed[NEXTUPDTIME_INDEX]);
		double nextUpdateLat = Double.parseDouble(parsed[NEXTLAT_INDEX]);
		double nextUpdateLong = Double.parseDouble(parsed[NEXTLONG_INDEX]);
		
		return new UserRecordInfo(userGUID, filename, homeLoclLat, homeLocLong,
				totalUpdates, nextUpdateNum, nextUpdateUnixTime,
				nextUpdateLat, nextUpdateLong );
	}
	
	public String toString()
	{
		String str = "";
		str = userGUID+","+filename
				+","+homeLoclLat+","+homeLocLong
				+","+totalUpdates+","+nextUpdateNum
				+","+nextUpdateUnixTime
				+","+nextUpdateLat+","+nextUpdateLong;
		return str;
	}
	
	public String getGUID()
	{
		return userGUID;
	}
	
	public String getFilename()
	{
		return this.filename;
	}
	
	public double getHomeLat()
	{
		return homeLoclLat;
	}
	
	public double getHomeLong()
	{
		return this.homeLocLong;
	}
	
	public int getTotalUpdates()
	{
		return this.totalUpdates;
	}
	
	public int getNextUpdateNum()
	{
		return this.nextUpdateNum;
	}
	
	public long getNextUpdateUnixTime()
	{
		return this.nextUpdateUnixTime;
	}
	
	public double getNextUpdateLat()
	{
		return this.nextUpdateLat;
	}
	
	public double getNextUpdateLong()
	{
		return this.nextUpdateLong;
	}
}
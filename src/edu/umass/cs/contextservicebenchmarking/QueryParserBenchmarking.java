package edu.umass.cs.contextservicebenchmarking;

import java.util.Random;

import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.utils.Utils;

public class QueryParserBenchmarking 
{
	public static int numAttrsInQuery 			= 4;
	public static int numAttrs        			= 20;
	
	public static double ATTR_MIN				= 1.0;
	public static double ATTR_MAX				= 1500.0;
	
	public static Random searchQueryRand		= new Random();
	
	public static String attrPrefix				= "attr";
	
	public static void main(String[] args)
	{
		ContextServiceConfig.configFileDirectory = args[0];
		AttributeTypes.initialize();
		long start = System.currentTimeMillis();
		for(int i = 0; i < 1000; i++)
		{
			String searchQuery = getQueryMessageWithSmallRanges();
			String groupGUID = Utils.getSHA1(searchQuery);
			QueryInfo<Integer> currReq  
				= new QueryInfo<Integer>( searchQuery, 0, groupGUID, i, 
					"127.0.0.1", 5000, 300000 );
		}
		long end = System.currentTimeMillis();
		
		System.out.println("parsing time "+(end-start));
	}
	
	private static String getQueryMessageWithSmallRanges()
	{
		String searchQuery
			= "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE ";
		
		int randAttrNum = -1;
		for( int i=0; i<numAttrsInQuery; i++)
		{
			// if num attrs and num in query are same then send query on all attrs
			if(numAttrs == numAttrsInQuery)
			{
				randAttrNum++;
			}
			else
			{
				randAttrNum = searchQueryRand.nextInt(numAttrs);
			}
						
			
			String attrName = attrPrefix+randAttrNum;
			double attrMin 
				= ATTR_MIN
				+searchQueryRand.nextDouble()*(ATTR_MAX - ATTR_MIN);
			
			// querying 10 % of domain
			double predLength 
				= (0.1*(ATTR_MAX - ATTR_MIN)) ;
			
			double attrMax = attrMin + predLength;
			//		double latitudeMax = latitudeMin 
			//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
			// making it curcular
			if( attrMax > ATTR_MAX )
			{
				double diff = attrMax - ATTR_MAX;
				attrMax = ATTR_MIN + diff;
			}
			// last so no AND
			if( i == (numAttrsInQuery-1) )
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
						+" <= "+attrMax;
			}
			else
			{
				searchQuery = searchQuery + " "+attrName+" >= "+attrMin+" AND "+attrName
					+" <= "+attrMax+" AND ";
			}
		}
		return searchQuery;
	}
}
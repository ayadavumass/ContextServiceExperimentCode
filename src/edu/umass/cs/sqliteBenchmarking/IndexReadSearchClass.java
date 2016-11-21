package edu.umass.cs.sqliteBenchmarking;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class IndexReadSearchClass extends AbstractRequestSendingClass
{
	private final Random queryRand;
	
	private final HashMap<Integer, HashMap<String, Boolean>> subspaceMap;
	
	private double sumResultSize = 0.0;
	
	private double sumAttrMatch = 0.0;
	
	public IndexReadSearchClass()
	{
		super(SQLiteThroughputBenchmarking.SEARCH_LOSS_TOLERANCE);
		subspaceMap = new HashMap<Integer, HashMap<String, Boolean>>();
		
		queryRand = new Random();
		readSubspaceInfo();
	}
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			searchQueryRateControlledRequestSender();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void readSubspaceInfo()
	{
		Connection myConn = null;
		Statement stmt = null;
		
		try
		{
			myConn = SQLiteThroughputBenchmarking.dsInst.getConnection(DataSource.SEARCH_POOL);
			stmt = myConn.createStatement();
			
			for( int i=0; i<SQLiteThroughputBenchmarking.NUM_SUBSPACES; i++ )
			{
				String str = "subspace num "+i;
				HashMap<String, Boolean> subspaceAttrMap = new HashMap<String, Boolean>();
				
				String tableName = "subspaceId"+i+"RepNum0PartitionInfo";
				String query = "select * from "+tableName;
				
				ResultSet rs = stmt.executeQuery(query);
				
				while( rs.next() )
				{
					ResultSetMetaData rsmd = rs.getMetaData();
					int columnCount = rsmd.getColumnCount();
					
					// The column count starts from 1
					for (int j = 1; j <= columnCount; j++ )
					{
						String colName = rsmd.getColumnName(j);
						
						// just checking the lowerAttr name attributes 
						// to get the attributes in subspace.
						if( colName.startsWith("lower") )
						{
							String attrName = colName.substring(5);
							subspaceAttrMap.put(attrName, true);
							str = str + " "+attrName;
						}
					}
					break;
				}
				rs.close();
				System.out.println(str);
				subspaceMap.put(i, subspaceAttrMap);
			}
		}
		catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		finally
		{
			try
			{
				if(stmt != null)
					stmt.close();
				
				if(myConn != null)
					myConn.close();
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	//String query 
	// = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE GeojsonOverlap(geoLocationCurrentLat, geoLocationCurrentLong, "+geoJSONObject.toString()+")";
	private void searchQueryRateControlledRequestSender() throws Exception
	{
		// as it is per ms
		double reqspms = SQLiteThroughputBenchmarking.requestsps/1000.0;
		long currTime  = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		while( ( (System.currentTimeMillis() - expStartTime)
				< SQLiteThroughputBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
//				JSONObject queryGeoJSON = 
//						weatherAlertsArray.get( queryRand.nextInt(weatherAlertsArray.size() ) );
//				int beg1 = this.queryRand.nextInt(1400);
//		    	int end1 = beg1+this.queryRand.nextInt(1500 - beg1-3);
//		    	
//		    	int beg2 = this.queryRand.nextInt(1400);
//		    	int end2 = beg2+this.queryRand.nextInt(1500 - beg2-3);
//		    	
//				String selectTableSQL = "SELECT nodeGUID from "+MySQLThroughputBenchmarking.tableName+" WHERE "
//				+ "( value1 >= "+beg1 +" AND value1 < "+end1+" AND "
//				+ " value2 >= "+beg2 +" AND value2 < "+end2+" )";
//				sendQueryMessage(selectTableSQL);
				sendQueryMessageWithSmallRanges();
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
//				int beg1 = this.queryRand.nextInt(1400);
//		    	int end1 = beg1+this.queryRand.nextInt(1500 - beg1-3);
//		    	
//		    	int beg2 = this.queryRand.nextInt(1400);
//		    	int end2 = beg2+this.queryRand.nextInt(1500 - beg2-3);
//		    	
//				String selectTableSQL = "SELECT nodeGUID from "+MySQLThroughputBenchmarking.tableName+" WHERE "
//				+ "( value1 >= "+beg1 +" AND value1 < "+end1+" AND "
//				+ " value2 >= "+beg2 +" AND value2 < "+end2+" )";
//				sendQueryMessage(selectTableSQL);
				sendQueryMessageWithSmallRanges();
				numSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("IndexReadSearch eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("IndexReadSearch result:Goodput "+sysThrput);
	}
	
//	private void sendQueryMessage(String mysqlQuery)
//	{
//		SearchTask searchTask = new SearchTask( mysqlQuery, this );
//		MySQLThroughputBenchmarking.taskES.execute(searchTask);
//	}
	
	private void sendQueryMessage()
	{
		String searchQuery
			= "SELECT nodeGUID FROM "+SQLiteThroughputBenchmarking.dataTableName
				+" WHERE ";
//			+ "geoLocationCurrentLat >= "+latitudeMin +" AND geoLocationCurrentLat <= "+latitudeMax 
//			+ " AND "
//			+ "geoLocationCurrentLong >= "+longitudeMin+" AND geoLocationCurrentLong <= "+longitudeMax;
		
		int randAttrNum = -1;
		for( int i=0; i<SQLiteThroughputBenchmarking.numAttrsInQuery; i++)
		{
			// if num attrs and num in query are same then send query on all attrs
			if(SQLiteThroughputBenchmarking.numAttrs == SQLiteThroughputBenchmarking.numAttrsInQuery)
			{
				randAttrNum++;
			}
			else
			{
				randAttrNum = queryRand.nextInt(SQLiteThroughputBenchmarking.numAttrs);
			}				
			
			String attrName = "attr"+randAttrNum;
			double attrMin 
				= 1
				+queryRand.nextDouble()*(SQLiteThroughputBenchmarking.ATTR_MAX - SQLiteThroughputBenchmarking.ATTR_MIN);
			
			double predLength 
				= (queryRand.nextDouble()*(SQLiteThroughputBenchmarking.ATTR_MAX - SQLiteThroughputBenchmarking.ATTR_MIN));
			
			double attrMax = attrMin + predLength;
			//		double latitudeMax = latitudeMin 
			//					+WeatherAndMobilityBoth.percDomainQueried*(WeatherAndMobilityBoth.LATITUDE_MAX - WeatherAndMobilityBoth.LATITUDE_MIN);
			// making it curcular
//			if( attrMax > MySQLThroughputBenchmarking.ATTR_MAX )
//			{
////				double diff = attrMax - MySQLThroughputBenchmarking.ATTR_MAX;
////				attrMax = MySQLThroughputBenchmarking.ATTR_MIN + diff;
//			}
			
			// last so no AND
			if(i == (SQLiteThroughputBenchmarking.numAttrsInQuery-1))
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
		SearchTask searchTask = new SearchTask( searchQuery, this );
		SQLiteThroughputBenchmarking.taskES.execute(searchTask);
	}
	
	private void sendQueryMessageWithSmallRanges()
	{
		HashMap<String, Boolean> attrMap = pickDistinctAttrs
				( SQLiteThroughputBenchmarking.numAttrsInQuery, 
						SQLiteThroughputBenchmarking.numAttrs, queryRand );
		
		OverlapClass oClass = getMaxOverlappingSubspaceNum(  attrMap );
		
		String tableName = "subspaceId"+oClass.subspaceNum+"RepNum0PartitionInfo";
		String selectTableSQL = "SELECT hashCode, respNodeID from "+tableName+" WHERE ";
		
		List<String> matchingAttrs = oClass.overlapAttrs;
		this.sumAttrMatch = sumAttrMatch + matchingAttrs.size();
		
		for( int i=0; i<matchingAttrs.size(); i++ )
		{
			String attrName = matchingAttrs.get(i);
			
			String lowerAttr = "lower"+attrName;
			String upperAttr = "upper"+attrName;
			
			double attrMin 
				= SQLiteThroughputBenchmarking.ATTR_MIN
					+queryRand.nextDouble()*(SQLiteThroughputBenchmarking.ATTR_MAX 
							- SQLiteThroughputBenchmarking.ATTR_MIN);
		
			// querying 10 % of domain
			double predLength 
				= (SQLiteThroughputBenchmarking.predicateLength*
						(SQLiteThroughputBenchmarking.ATTR_MAX 
						- SQLiteThroughputBenchmarking.ATTR_MIN)) ;
		
			double attrMax = attrMin + predLength;
			
			if( attrMax > SQLiteThroughputBenchmarking.ATTR_MAX )
			{
				double diff = attrMax - SQLiteThroughputBenchmarking.ATTR_MAX;
				attrMax = SQLiteThroughputBenchmarking.ATTR_MIN + diff;
			}
			
			if( attrMin <= attrMax )
			{
				String queryMin  =  attrMin + "";
				String queryMax  =  attrMax + "";
				
				// three cases to check, documentation
				// trying to find if there is an overlap in the ranges, 
				// the range specified by user and the range in database.
				// overlap is there if queryMin lies between the range in database
				// or queryMax lies between the range in database.
				// So, we specify two or conditions.
				// for right side value, it can't be equal to rangestart, 
				// but it can be equal to rangeEnd, although even then it doesn't include
				// rangeEnd.
				// or the range lies in between the queryMin and queryMax
				
				// follwing the convention that the in (lowerVal, upperVal) range lowerVal is included in 
				// range and upperVal is not included in range. This convnetion is for data storage in mysql
				// queryMin and queryMax aare always both end points included.
				// means a query >= queryMin and query <= queryMax, but never query > queryMin and query < queryMax
				selectTableSQL = selectTableSQL +" ( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" ) ";
			}
			else // when lower value in query predicate is greater than upper value, meaning circular query, 
				// it is done mostly for generating uniform workload for experiments
			{
				// first case from lower to max value
				String queryMin  =  attrMin + "";
				String queryMax  =  SQLiteThroughputBenchmarking.ATTR_MAX + "";
				
				selectTableSQL = selectTableSQL +"( ( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" ) OR ";
				
				// second case from minvalue to upper val
				queryMin  =  SQLiteThroughputBenchmarking.ATTR_MIN + "";
				queryMax  =  attrMax + "";
				selectTableSQL = selectTableSQL +"( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" )  )";
			}
			
			if( i != (matchingAttrs.size()-1) )
			{
				selectTableSQL = selectTableSQL + " AND ";
			}
		}
		
		IndexReadSearchTask searchTask = new IndexReadSearchTask( selectTableSQL, this );
		SQLiteThroughputBenchmarking.taskES.execute(searchTask);
	}
	
	public double getAvgAttrMatch()
	{
		return this.sumAttrMatch/numRecvd;
	}
	
	private HashMap<String, Boolean> pickDistinctAttrs( int numAttrsToPick, 
			int totalAttrs, Random randGen )
	{
		HashMap<String, Boolean> hashMap = new HashMap<String, Boolean>();
		int currAttrNum = 0;
		while(hashMap.size() != numAttrsToPick)
		{
			if(SQLiteThroughputBenchmarking.numAttrs == SQLiteThroughputBenchmarking.numAttrsInQuery)
			{
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);				
				currAttrNum++;
			}
			else
			{
				currAttrNum = randGen.nextInt(SQLiteThroughputBenchmarking.numAttrs);
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
			}
		}
		return hashMap;
	}
	
	
	private OverlapClass getMaxOverlappingSubspaceNum( HashMap<String, Boolean> attrMap )
	{
		int maxOverlap = -1;
		OverlapClass maxOverlapClass = null;
		
		Iterator<Integer> subspaceIter = subspaceMap.keySet().iterator();
		
		while( subspaceIter.hasNext() )
		{
			int subspaceNum = subspaceIter.next();
			HashMap<String, Boolean> subAttrMap = subspaceMap.get(subspaceNum);
			int currAttrOverlap = 0;
			
			List<String> currOverlapAttr = new LinkedList<String>();
			
			Iterator<String> attrIter = attrMap.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String qAttrName = attrIter.next();
				if( subAttrMap.containsKey( qAttrName ) )
				{
					currAttrOverlap++;
					currOverlapAttr.add( qAttrName );
				}
			}
			
			
			if( maxOverlap < currAttrOverlap )
			{
				maxOverlap = currAttrOverlap;
				OverlapClass overlapClass = new OverlapClass();
				overlapClass.subspaceNum = subspaceNum;
				overlapClass.overlapAttrs = currOverlapAttr;
				maxOverlapClass = overlapClass;
			}
		}
		assert(maxOverlapClass != null);
		return maxOverlapClass;
	}
	
	
	private class OverlapClass
	{
		int subspaceNum;
		List<String> overlapAttrs;
	}
	
	public double getAvgResultSize()
	{
		return sumResultSize/numRecvd;
	}
	
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			sumResultSize = sumResultSize + resultSize;
//			System.out.println("IndexReadSearch reply recvd size "+resultSize+" time taken "+timeTaken+
//					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
	
	
//	private void createIndexTables()
//	{
//		Connection myConn = null;
//		Statement stmt = null;
//		
//		try
//		{
//			myConn = MySQLThroughputBenchmarking.dsInst.getConnection();
//			stmt = myConn.createStatement();
//			
//			Iterator<Integer> subspaceIter = subspaceMap.keySet().iterator();
//			
//			while( subspaceIter.hasNext() )
//			{
//				int subspaceNum = subspaceIter.next();
//				HashMap<String, Boolean> attrMap = subspaceMap.get(subspaceNum);
//				
//				String indexTableName = indexTablePrefix+subspaceNum;
//				
//				String newTableCommand = "create table "+indexTableName+" ( hashCode INTEGER PRIMARY KEY , "
//					      + "   respNodeID INTEGER ";
//				
//				Iterator<String> attrIter = attrMap.keySet().iterator();
//				
//				while( attrIter.hasNext() )
//				{
//					String attrName = attrIter.next();
//					
//					String mySQLDataType = "DOUBLE";
//					// lower range of this attribute in this subspace
//					String lowerAttrName = "lower"+attrName;
//					String upperAttrName = "upper"+attrName;
//					
//					newTableCommand = newTableCommand + " , "+lowerAttrName+" "+mySQLDataType+" , "+upperAttrName+" "+mySQLDataType+" , "
//							+ "INDEX USING BTREE("+lowerAttrName+" , "+upperAttrName+")";
//				}
//				newTableCommand = newTableCommand +" )";
//				stmt.executeUpdate(newTableCommand);
//			}
//		}
//		catch ( SQLException e )
//		{
//			e.printStackTrace();
//		} 
//		finally
//		{
//			try
//			{
//				if(stmt != null)
//					stmt.close();
//				
//				if(myConn != null)
//					myConn.close();
//			}
//			catch (SQLException e)
//			{
//				e.printStackTrace();
//			}
//		}
//	}
}
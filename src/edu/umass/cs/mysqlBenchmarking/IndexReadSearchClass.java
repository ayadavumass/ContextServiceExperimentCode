package edu.umass.cs.mysqlBenchmarking;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

public class IndexReadSearchClass extends AbstractRequestSendingClass
{
	private final Random queryRand;
	
	private final HashMap<Integer, HashMap<String, Boolean>> subspaceMap;
	
	private double sumResultSize = 0.0;
	
	public IndexReadSearchClass()
	{
		super(MySQLThroughputBenchmarking.SEARCH_LOSS_TOLERANCE);
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
			myConn = MySQLThroughputBenchmarking.dsInst.getConnection();
			stmt = myConn.createStatement();
			
			for( int i=0; i<MySQLThroughputBenchmarking.NUM_SUBSPACES; i++ )
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
		double reqspms = MySQLThroughputBenchmarking.requestsps/1000.0;
		long currTime  = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		while( ( (System.currentTimeMillis() - expStartTime)
				< MySQLThroughputBenchmarking.EXPERIMENT_TIME ) )
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
			= "SELECT nodeGUID FROM "+MySQLThroughputBenchmarking.tableName+" WHERE ";
//			+ "geoLocationCurrentLat >= "+latitudeMin +" AND geoLocationCurrentLat <= "+latitudeMax 
//			+ " AND "
//			+ "geoLocationCurrentLong >= "+longitudeMin+" AND geoLocationCurrentLong <= "+longitudeMax;
		
		int randAttrNum = -1;
		for( int i=0; i<MySQLThroughputBenchmarking.numAttrsInQuery; i++)
		{
			// if num attrs and num in query are same then send query on all attrs
			if(MySQLThroughputBenchmarking.numAttrs == MySQLThroughputBenchmarking.numAttrsInQuery)
			{
				randAttrNum++;
			}
			else
			{
				randAttrNum = queryRand.nextInt(MySQLThroughputBenchmarking.numAttrs);
			}				
			
			String attrName = "attr"+randAttrNum;
			double attrMin 
				= 1
				+queryRand.nextDouble()*(MySQLThroughputBenchmarking.ATTR_MAX - MySQLThroughputBenchmarking.ATTR_MIN);
			
			double predLength 
				= (queryRand.nextDouble()*(MySQLThroughputBenchmarking.ATTR_MAX - MySQLThroughputBenchmarking.ATTR_MIN));
			
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
			if(i == (MySQLThroughputBenchmarking.numAttrsInQuery-1))
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
//		SearchTask searchTask = new SearchTask( searchQuery, new JSONArray(), this );
//		SearchAndUpdateDriver.taskES.execute(searchTask);
		
		SearchTask searchTask = new SearchTask( searchQuery, this );
		MySQLThroughputBenchmarking.taskES.execute(searchTask);
		
//		ExperimentSearchReply searchRep 
//					= new ExperimentSearchReply( reqIdNum );
//		SearchAndUpdateDriver.csClient.sendSearchQueryWithCallBack
//					(searchQuery, 300000, searchRep, this.getCallBack());
	}
	
	private void sendQueryMessageWithSmallRanges()
	{
		int numAttrsMatching 
			= 1+queryRand.nextInt(MySQLThroughputBenchmarking.numAttrsInQuery);
		
		int numAttrsPerSubspace 
			= MySQLThroughputBenchmarking.numAttrs/MySQLThroughputBenchmarking.NUM_SUBSPACES;
		
		if(numAttrsMatching > numAttrsPerSubspace)
		{
			numAttrsMatching = numAttrsPerSubspace;
		}
		
		int subspaceNum = 0;
		
		String tableName = "subspaceId"+subspaceNum+"RepNum0PartitionInfo";
		String selectTableSQL = "SELECT hashCode, respNodeID from "+tableName+" WHERE ";
		
		HashMap<String, Boolean> subspaceAttrMap = subspaceMap.get(subspaceNum);
		Iterator<String> subspaceAttrIter = subspaceAttrMap.keySet().iterator();
		
		int currNumAttrs = 0;
		while( subspaceAttrIter.hasNext() )
		{
			String attrName = subspaceAttrIter.next();
			
			String lowerAttr = "lower"+attrName;
			String upperAttr = "upper"+attrName;
			
			double attrMin 
				= MySQLThroughputBenchmarking.ATTR_MIN
					+queryRand.nextDouble()*(MySQLThroughputBenchmarking.ATTR_MAX 
							- MySQLThroughputBenchmarking.ATTR_MIN);
		
			// querying 10 % of domain
			double predLength 
				= (0.1*(MySQLThroughputBenchmarking.ATTR_MAX 
						- MySQLThroughputBenchmarking.ATTR_MIN)) ;
		
			double attrMax = attrMin + predLength;
			
			if( attrMax > MySQLThroughputBenchmarking.ATTR_MAX )
			{
				double diff = attrMax - MySQLThroughputBenchmarking.ATTR_MAX;
				attrMax = MySQLThroughputBenchmarking.ATTR_MIN + diff;
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
				String queryMax  =  MySQLThroughputBenchmarking.ATTR_MAX + "";
				
				selectTableSQL = selectTableSQL +"( ( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" ) OR ";
				
				// second case from minvalue to upper val
				queryMin  =  MySQLThroughputBenchmarking.ATTR_MIN + "";
				queryMax  =  attrMax + "";
				selectTableSQL = selectTableSQL +"( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" )  )";
			}
			currNumAttrs++;
			if( currNumAttrs != numAttrsMatching )
			{
				selectTableSQL = selectTableSQL + " AND ";
			}
			
			if( currNumAttrs == numAttrsMatching )
				break;
		}
		
		IndexReadSearchTask searchTask = new IndexReadSearchTask( selectTableSQL, this );
		MySQLThroughputBenchmarking.taskES.execute(searchTask);
	}
	
	
//	private OverlapClass getMaxOverlappingSubspaceNum(List<String> attrList)
//	{
//		int maxOverlap = -1;
//		OverlapClass maxOverlapClass = null;
//		
//		Iterator<Integer> subspaceIter = subspaceMap.keySet().iterator();
//		
//		while( subspaceIter.hasNext() )
//		{
//			int subspaceNum = subspaceIter.next();
//			HashMap<String, Boolean> subAttrMap = subspaceMap.get(subspaceNum);
//			int currAttrOverlap = 0;
//			
//			List<String> currOverlapAttr = new LinkedList<String>();
//			
//			for( int i=0; i < attrList.size(); i++ )
//			{
//				if( subAttrMap.containsKey(attrList.get(i)) )
//				{
//					currAttrOverlap++;
//					currOverlapAttr.add( attrList.get(i) );
//				}
//			}
//			
//			if( maxOverlap < currAttrOverlap )
//			{
//				maxOverlap = currAttrOverlap;
//				OverlapClass overlapClass = new OverlapClass();
//				overlapClass.subspaceNum = subspaceNum;
//				overlapClass.overlapAttrs = currOverlapAttr;
//				maxOverlapClass = overlapClass;
//			}
//		}
//		assert(maxOverlapClass != null);
//		return maxOverlapClass;
//	}
//	
//	
//	private class OverlapClass
//	{
//		int subspaceNum;
//		List<String> overlapAttrs;
//	}
	
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
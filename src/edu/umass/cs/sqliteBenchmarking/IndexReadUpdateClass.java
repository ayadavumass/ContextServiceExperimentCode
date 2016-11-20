package edu.umass.cs.sqliteBenchmarking;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;


public class IndexReadUpdateClass extends AbstractRequestSendingClass
{
	private final Random updateRand;
	public double sumTime = 0.0;
	private final HashMap<Integer, HashMap<String, Boolean>> subspaceMap;
	
	public IndexReadUpdateClass()
	{
		super(SQLiteThroughputBenchmarking.SEARCH_LOSS_TOLERANCE);
		subspaceMap = new HashMap<Integer, HashMap<String, Boolean>>();
		
		updateRand = new Random();
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
			myConn = SQLiteThroughputBenchmarking.dsInst.getConnection();
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
				checkForUpdateOverlap();
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
				checkForUpdateOverlap();
				numSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("IndexReadUpdate eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("IndexReadUpdate result:Goodput "+sysThrput);
	}
	
	
	private void checkForUpdateOverlap()
	{
		int subspaceNum = 0;
		String tableName = "subspaceId"+subspaceNum+"RepNum0PartitionInfo";
		String selectTableSQL = "SELECT hashCode, respNodeID from "+tableName+" WHERE ";
		
		HashMap<String, Boolean> subspaceAttrMap = subspaceMap.get(subspaceNum);
		Iterator<String> attrIter = subspaceAttrMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			String lowerAttr = "lower"+attrName;
			String upperAttr = "upper"+attrName;
			
			double attrMin 
				= SQLiteThroughputBenchmarking.ATTR_MIN
					+updateRand.nextDouble()*(SQLiteThroughputBenchmarking.ATTR_MAX 
							- SQLiteThroughputBenchmarking.ATTR_MIN);
		
			double attrMax = attrMin;
			
//			if(AttributeTypes.compareTwoValues(qcomponent.getLowerBound(),
//					qcomponent.getUpperBound(), dataType))
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
				
				// following the convention that the in (lowerVal, upperVal) range lowerVal is included in 
				// range and upperVal is not included in range. This convnetion is for data storage in mysql
				// queryMin and queryMax aare always both end points included.
				// means a query >= queryMin and query <= queryMax, but never query > queryMin and query < queryMax
				selectTableSQL = selectTableSQL +" ( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" ) ";
			}
			
			if( attrIter.hasNext() )
			{
				selectTableSQL = selectTableSQL + " AND ";
			}
		}
		
		IndexReadUpdateTask updateTask = new IndexReadUpdateTask( selectTableSQL, this );
		SQLiteThroughputBenchmarking.taskES.execute(updateTask);
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken)
	{
	}
	
	public double getAvgTime()
	{
		return this.sumTime/numRecvd;
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			this.sumTime = this.sumTime + timeTaken;
//			System.out.println("IndexReadUpdate reply recvd size "+resultSize
//					+" time taken "+timeTaken
//					+" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
}
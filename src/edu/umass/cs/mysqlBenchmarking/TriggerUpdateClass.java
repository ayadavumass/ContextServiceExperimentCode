package edu.umass.cs.mysqlBenchmarking;



import java.util.Iterator;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class TriggerUpdateClass extends AbstractRequestSendingClass
{
	private Random updateRand;
	
	private double sumUpdTime = 0;
	private double sumRemoved = 0.0;
	private double sumAdded = 0.0;
	
	public TriggerUpdateClass()
	{
		super(MySQLThroughputBenchmarking.UPD_LOSS_TOLERANCE);
		updateRand = new Random();
	}
	
	@Override
	public void run()
	{
		try 
		{
			this.startExpTime();
			updRateControlledRequestSender();
		} catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	private void updRateControlledRequestSender() throws Exception
	{
		double reqspms = MySQLThroughputBenchmarking.requestsps/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 0;
		
		//while( ( totalNumUsersSent < numUsers ) )
		while( ( (System.currentTimeMillis() - expStartTime) 
						< MySQLThroughputBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				currUserGuidNum=((int)currUserGuidNum)%MySQLThroughputBenchmarking.numGuids;
				//numSent++;
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
				doUpdate((int)currUserGuidNum);
				currUserGuidNum++;
				currUserGuidNum=((int)currUserGuidNum)%MySQLThroughputBenchmarking.numGuids;
				//numSent++;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Update eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Update result:Goodput "+sysThrput);
	}
	
	private void doUpdate(int currUserGuidNum) throws JSONException
	{
		numSent++;
		String guid = MySQLThroughputBenchmarking.getSHA1
				(MySQLThroughputBenchmarking.guidPrefix+currUserGuidNum);
		String attrName = "attr"+updateRand.nextInt(MySQLThroughputBenchmarking.numAttrs);
		double value = 1500*+updateRand.nextDouble();
		JSONObject updateJSON = new JSONObject();
		
		updateJSON.put(attrName, value);
		
		JSONObject oldJSON = new JSONObject();
		for(int i=0; i<MySQLThroughputBenchmarking.numAttrs; i++)
		{
			value = 1500*+updateRand.nextDouble();
			oldJSON.put("attr"+i, value);
		}
		String removedGrpQuery = returnRemovedGroupGUIDsQuery
							(oldJSON, updateJSON);
		
		String newGrpQuery = returnAddedGroupGUIDs( 
				oldJSON, updateJSON );
		
		JSONArray removedGroups = new JSONArray();
		JSONArray addedGroups = new JSONArray();
		
		TriggerUpdateTask updTask = new TriggerUpdateTask( removedGrpQuery, newGrpQuery, 
				removedGroups, addedGroups, this );
		MySQLThroughputBenchmarking.taskES.execute(updTask);
	}
	
	
	private String returnRemovedGroupGUIDsQuery(JSONObject oldValJSON, JSONObject updateValJSON) throws JSONException
	{
		assert(oldValJSON != null);
		assert(oldValJSON.length() > 0);
		
		//FIXME: DONE: it could be changed to calculating to be removed GUIDs right here.
		// in one single mysql query once can check to old group guid and new group guids
		// and return groupGUIDs which are in old value but no in new value.
		// but usually complex queries have more cost, so not sure if it would help.
		// but this optimization can be checked later on if number of group guids returned becomes 
		// an issue later on. 
		String queriesWithAttrs 
			= getQueriesThatContainAttrsInUpdate(updateValJSON);
		//String newTableName = "projTable";
		
		//String createTempTable = "CREATE TEMPORARY TABLE "+
		//		newTableName+" AS ( "+queriesWithAttrs+" ) ";
		
		String oldGroupsQuery 
			= getQueryToGetOldValueGroups(oldValJSON);
		
		String newGroupsQuery = getQueryToGetNewValueGroups
				( oldValJSON, updateValJSON );
		
		String removedGroupQuery = "SELECT groupGUID, userIP, userPort FROM "
				+ MySQLThroughputBenchmarking.triggerTableName
				+ " WHERE "
				+ " groupGUID IN ( "+queriesWithAttrs+" ) AND "
			    + " groupGUID IN ( "+oldGroupsQuery+" ) AND "
			    + " groupGUID NOT IN ( "+ newGroupsQuery+" ) ";
		
		return removedGroupQuery;
	}
		
	private String returnAddedGroupGUIDs( 
			JSONObject oldValJSON, JSONObject updateValJSON )
	{
		String tableName 			= MySQLThroughputBenchmarking.triggerTableName;
		
		// for groups associated with the new value
		try
		{	
			String selectQuery ="";
			
			String queriesWithAttrs = getQueriesThatContainAttrsInUpdate(updateValJSON);
			
			selectQuery = "SELECT groupGUID, userIP, userPort FROM "+tableName
					+" WHERE ";
			
			String newGroupsQuery = 
					getQueryToGetNewValueGroups( oldValJSON, updateValJSON);
			
			String oldGroupsQuery 
				= getQueryToGetOldValueGroups(oldValJSON);
			
			selectQuery = selectQuery 
					+ " groupGUID IN ( "+queriesWithAttrs+" ) AND "
					+ " groupGUID NOT IN ( "+oldGroupsQuery+" ) AND "
					+ " groupGUID IN ( "+newGroupsQuery+" ) ";
			
			return selectQuery;
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return "";
	}
		
		
	/**
	 * Returns search queries that contain attributes of an update, 
	 * as only those search queries can be affected.
	 * This helps in reducing the size of the search queries that needs to be checked
	 * further if the GUID in update satisfies that or not.
	 * @param attrsInUpdate
	 * @return
	 */
	public static String getQueriesThatContainAttrsInUpdate( JSONObject attrsInUpdate)
	{
		String tableName 			= MySQLThroughputBenchmarking.triggerTableName;
		
		String selectQuery 			= "SELECT groupGUID FROM "+tableName+" WHERE ";
		
		Iterator<String> attrIter 	= attrsInUpdate.keys();
		boolean first = true;
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			if( first )
			{
				String lowerAttrName = "lower"+attrName;
				selectQuery = selectQuery + lowerAttrName +" != 0";
				first = false;
			}
			else
			{
				String lowerAttrName = "lower"+attrName;
				selectQuery = selectQuery +" AND "+ lowerAttrName +" != 0";
			}
		}
		return selectQuery;
	}
	
	
	public static String getQueryToGetOldValueGroups(JSONObject oldValJSON) 
			throws JSONException
	{
		String tableName 			= MySQLThroughputBenchmarking.triggerTableName;
		
		//		attrSubspaceInfo.keySet().iterator();
		// for groups associated with old value
		boolean first = true;
		String selectQuery = "SELECT groupGUID FROM "+tableName+" WHERE ";
		
		//while( attrIter.hasNext() )
		for( int i=0; i<MySQLThroughputBenchmarking.numAttrs; i++ )
		{
			String currAttrName = "attr"+i;
			
			String minVal = MySQLThroughputBenchmarking.ATTR_MIN+"";
			String maxVal = MySQLThroughputBenchmarking.ATTR_MAX+"";
			
			String attrValForMysql = oldValJSON.getString(currAttrName);
			
			
			String lowerValCol = "lower"+currAttrName;
			String upperValCol = "upper"+currAttrName;
			//FIXED: for circular queries, this won't work.
			if( first )
			{
				// <= and >= both to handle the == case of the default value
				if(MySQLThroughputBenchmarking.disableCircularQueryTrigger)
				{
					selectQuery = selectQuery+lowerValCol+" <= "+attrValForMysql
					+" AND "+upperValCol+" >= "+attrValForMysql;
					
				}
				else
				{
					selectQuery = selectQuery + " ( ( "+lowerValCol+" <= "+attrValForMysql
							+" AND "+upperValCol+" >= "+attrValForMysql+" ) OR "
									+ " ( ( "+lowerValCol+" > "+upperValCol+") AND "
											+ " ( ( " +minVal+" <= "+attrValForMysql
											+" AND "+upperValCol+" >= "+attrValForMysql+" ) "
									+ "OR ( "+ lowerValCol+" <= "+attrValForMysql
									+" AND "+maxVal+" >= "+attrValForMysql+" ) " + " ) "+ " ) )";
				}
				first = false;
			}
			else
			{
				// old query without circular predicates
//				selectQuery = selectQuery+" AND "+lowerValCol+" <= "+attrValForMysql
//						+" AND "+upperValCol+" >= "+attrValForMysql;
				
				if(MySQLThroughputBenchmarking.disableCircularQueryTrigger)
				{
					selectQuery = selectQuery+" AND "+lowerValCol+" <= "+attrValForMysql
							+" AND "+upperValCol+" >= "+attrValForMysql;
				}
				else
				{
					selectQuery = selectQuery + " AND ( ( "+lowerValCol+" <= "+attrValForMysql
							+" AND "+upperValCol+" >= "+attrValForMysql+" ) OR "
									+ " ( ( "+lowerValCol+" > "+upperValCol+") AND "
											+ " ( ( " +minVal+" <= "+attrValForMysql
											+" AND "+upperValCol+" >= "+attrValForMysql+" ) "
									+ "OR ( "+ lowerValCol+" <= "+attrValForMysql
									+" AND "+maxVal+" >= "+attrValForMysql+" ) " + " ) "+ " ) )";
				}
			}
		}
		return selectQuery;
	}
	
	
	public static String getQueryToGetNewValueGroups
		( JSONObject oldValJSON, JSONObject updateJSON ) 
					throws JSONException
	{
		String tableName 			= MySQLThroughputBenchmarking.triggerTableName;

		//Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		// for groups associated with the new value
		try
		{
			boolean first = true;
			String selectQuery = "SELECT groupGUID FROM "
					+tableName+" WHERE ";
			for( int i=0 ; i<MySQLThroughputBenchmarking.numAttrs; i++ )
			{
				String currAttrName = "attr"+i;
				
				String minVal = MySQLThroughputBenchmarking.ATTR_MIN+"";
				String maxVal = MySQLThroughputBenchmarking.ATTR_MAX+"";
				
				String attrValForMysql = 0+"";
	
				if( updateJSON.has(currAttrName) )
				{
					attrValForMysql = updateJSON.getString(currAttrName);
				}
				else if( oldValJSON.has(currAttrName) )
				{
					attrValForMysql = oldValJSON.getString(currAttrName);	
				}
	
				String lowerValCol = "lower"+currAttrName;
				String upperValCol = "upper"+currAttrName;
				//FIXED: will not work for circular queries
				if( first )
				{
					// <= and >= both to handle the == case of the default value
			//		selectQuery = selectQuery + lowerValCol+" <= "+attrValForMysql
			//				+" AND "+upperValCol+" >= "+attrValForMysql;
					
					if(MySQLThroughputBenchmarking.disableCircularQueryTrigger)
					{
						selectQuery = selectQuery + lowerValCol+" <= "+attrValForMysql
								+" AND "+upperValCol+" >= "+attrValForMysql;
					}
					else
					{
						selectQuery = selectQuery + " ( ( "+lowerValCol+" <= "+attrValForMysql
								+" AND "+upperValCol+" >= "+attrValForMysql+" ) OR "
										+ " ( ( "+lowerValCol+" > "+upperValCol+") AND "
												+ " ( ( " +minVal+" <= "+attrValForMysql
												+" AND "+upperValCol+" >= "+attrValForMysql+" ) "
										+ "OR ( "+ lowerValCol+" <= "+attrValForMysql
										+" AND "+maxVal+" >= "+attrValForMysql+" ) " + " ) "+ " ) )";
					}
					
					first = false;
				}
				else
				{
			//		selectQuery = selectQuery+" AND "+lowerValCol+" <= "+attrValForMysql
			//				+" AND "+upperValCol+" >= "+attrValForMysql;
					
					if(MySQLThroughputBenchmarking.disableCircularQueryTrigger)
					{
						selectQuery = selectQuery+" AND "+lowerValCol+" <= "+attrValForMysql
								+" AND "+upperValCol+" >= "+attrValForMysql;
					}
					else
					{
						selectQuery = selectQuery + " AND ( ( "+lowerValCol+" <= "+attrValForMysql
								+" AND "+upperValCol+" >= "+attrValForMysql+" ) OR "
										+ " ( ( "+lowerValCol+" > "+upperValCol+") AND "
												+ " ( ( " +minVal+" <= "+attrValForMysql
												+" AND "+upperValCol+" >= "+attrValForMysql+" ) "
										+ "OR ( "+ lowerValCol+" <= "+attrValForMysql
										+" AND "+maxVal+" >= "+attrValForMysql+" ) " + " ) "+ " ) )";
					}
				}
			}
			return selectQuery;
		}
		catch (JSONException e) 
		{
			e.printStackTrace();
		}
		assert(false);
		return "";
	}
	
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			sumUpdTime = sumUpdTime+timeTaken;
			String[] parsed = userGUID.split("-");
			this.sumRemoved = this.sumRemoved + Double.parseDouble(parsed[0]);
			this.sumAdded = this.sumAdded + Double.parseDouble(parsed[1]);
//			System.out.println("Update reply recvd "+userGUID+" time taken "+timeTaken+
//					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
	public double getAvgUpdateTime()
	{
		return sumUpdTime/numRecvd;
	}
	
	public double getAvgRemoved()
	{
		return this.sumRemoved/numRecvd;
	}
	
	public double getAvgAdded()
	{
		return this.sumAdded/numRecvd;
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken) 
	{	
	}
}
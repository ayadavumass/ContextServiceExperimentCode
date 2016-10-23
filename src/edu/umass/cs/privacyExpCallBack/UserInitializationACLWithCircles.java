package edu.umass.cs.privacyExpCallBack;


import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

public class UserInitializationACLWithCircles extends 
									AbstractRequestSendingClass
{
	// different random generator for each variable, as using one for 
	// all of them doesn't give uniform properties.
	
	private final Random initRand;
	//private final KeyPairGenerator kpg;
	private final Random aclRand;
	private final Random circleRand;
	
	public UserInitializationACLWithCircles() throws Exception
	{
		super( SearchAndUpdateDriver.INSERT_LOSS_TOLERANCE );
		initRand = new Random(SearchAndUpdateDriver.myID*100);
		aclRand  = new Random((SearchAndUpdateDriver.myID+1)*102);
		
		
		circleRand = new Random((SearchAndUpdateDriver.myID+1)*105);
//		kpg = KeyPairGenerator.getInstance
//					( ContextServiceConfig.AssymmetricEncAlgorithm );
		
		// just generate all user entries.
		generateUserEntries();
	}
	
	private void generateUserEntries() throws Exception
	{
		System.out.println("generateUserEntries started "
									+SearchAndUpdateDriver.numUsers);
		
		// generate guids
		for( int i=0; i < SearchAndUpdateDriver.numUsers; i++ )
		{
			int guidNum = i;
			InitTask currT = new InitTask(guidNum);
			SearchAndUpdateDriver.taskES.execute(currT);
		}
		
		synchronized(SearchAndUpdateDriver.usersVector)
		{
			while(SearchAndUpdateDriver.usersVector.size() != SearchAndUpdateDriver.numUsers)
			{
				SearchAndUpdateDriver.usersVector.wait();
			}
		}
		
		System.out.println("Guid creation complete");
		
		// generate ACLs.
		for( int i=0; i < SearchAndUpdateDriver.numUsers; i++ )
		{
			UserEntry currUserEntry 
						= SearchAndUpdateDriver.usersVector.get(i);
			
			int totalDistinctGuidsNeeded 
						= SearchAndUpdateDriver.totalACLMems;
			
			HashMap<String, ACLEntry> distinctGuidMap 
									= new HashMap<String, ACLEntry>();
			
			// generate classes
			while( distinctGuidMap.size() != totalDistinctGuidsNeeded )
			{
				int randIndex = aclRand.nextInt( 
						SearchAndUpdateDriver.usersVector.size() );
				
				GuidEntry randGuidEntry 
						= SearchAndUpdateDriver.usersVector.get(randIndex).getGuidEntry();
				byte[] guidACLMember 
						= Utils.hexStringToByteArray(randGuidEntry.getGuid());
				byte[] publicKeyBytes 
						= randGuidEntry.getPublicKey().getEncoded();
				
				ACLEntry aclEntry 
							= new ACLEntry(guidACLMember, publicKeyBytes);
				distinctGuidMap.put( Utils.byteArrayToHex(guidACLMember), 
						aclEntry );
				//unionACLEntry.add(aclEntry);
			}
			
			HashMap<Integer, List<ACLEntry>> circlesMap 
								= new HashMap<Integer, List<ACLEntry>>();
			
			Iterator<String> guidMapIter 
								= distinctGuidMap.keySet().iterator();
			
			int circlenum = 0;
			
			while( guidMapIter.hasNext() )
			{
				String guidACLMemberString = guidMapIter.next();
				ACLEntry currACLEntry = distinctGuidMap.get(guidACLMemberString);
				
				List<ACLEntry> circleGUIDs = 
						circlesMap.get(circlenum);
				
				if( circleGUIDs == null )
				{
					circleGUIDs = new LinkedList<ACLEntry>();
					circleGUIDs.add(currACLEntry);
					circlesMap.put(circlenum, circleGUIDs);
				}
				else
				{
					circleGUIDs.add(currACLEntry);
				}
				circlenum++;
				circlenum = circlenum%SearchAndUpdateDriver.numCircles; 
			}
			
			// probablistically add each guid to one more circle
			while( guidMapIter.hasNext() )
			{
				circlenum = circleRand.nextInt(SearchAndUpdateDriver.numCircles);
				String guidACLMemberString = guidMapIter.next();
				ACLEntry currACLEntry = distinctGuidMap.get(guidACLMemberString);
				
				List<ACLEntry> circleGUIDs = 
									circlesMap.get(circlenum);
				
				if(circleRand.nextDouble() <= SearchAndUpdateDriver.overlapProbability)
				{
					circleGUIDs.add(currACLEntry);
				}
			}
			currUserEntry.setCirclesMap(circlesMap);
			
			// generate ACLs
			HashMap<String, List<ACLEntry>> aclMap 
						= new HashMap<String, List<ACLEntry>>();
			
			generateACLWithFixedCircles( aclMap, 
					circlesMap, SearchAndUpdateDriver.numCirclesInACL );
			
			currUserEntry.setACLMap( aclMap );
			
			// generate anonymized IDs
			List<AnonymizedIDEntry> anonymizedIDList = 
					SearchAndUpdateDriver.csClient.computeAnonymizedIDs
					(currUserEntry.getGuidEntry(), aclMap, true);
			
			if( anonymizedIDList != null )
			{
				System.out.println( "Number of anonymized IDs created "+
						anonymizedIDList.size() );
//				for(int k=0; k<anonymizedIDList.size(); k++)
//				{
//					System.out.println("UserNum"+i+" "+
//									anonymizedIDList.get(k).toString());
//				}
			}
			currUserEntry.setAnonymizedIDList(anonymizedIDList);
		}
	}
	
	private HashMap<Integer, Boolean> pickDistinctCircles( int totalCircles, 
													int numCirclesToPick )
	{
		assert(numCirclesToPick <= totalCircles);
		HashMap<Integer, Boolean> pickedCircleMap 
									= new HashMap<Integer, Boolean>();
		
		while( pickedCircleMap.size() != numCirclesToPick )
		{
			int circleNum = circleRand.nextInt(totalCircles);
			
			if( pickedCircleMap.containsKey(circleNum) )
			{
				continue;
			}
			else
			{
				pickedCircleMap.put(circleNum, true);
			}
		}
		return pickedCircleMap;
	}
	
	
	private void generateACLWithFixedCircles( HashMap<String, List<ACLEntry>> aclMap, 
			HashMap<Integer, List<ACLEntry>> circlesMap, int numberCirlcesInACL )
	{
		for( int j=0; j < SearchAndUpdateDriver.numAttrs; j++ )
		{
			List<ACLEntry> attrACL  = new LinkedList<ACLEntry>();
			HashMap<Integer, Boolean> pickDistinctCirlces = 
						pickDistinctCircles( circlesMap.size(), numberCirlcesInACL );
			
			Iterator<Integer> iter 
									= pickDistinctCirlces.keySet().iterator();
			
			while( iter.hasNext() )
			{
				int currCircleNum = iter.next();
				List<ACLEntry> classMemberList = circlesMap.get(currCircleNum);
				
				for( int l = 0; l < classMemberList.size(); l++ )
				{
					ACLEntry aclEntry = classMemberList.get(l);
					attrACL.add(aclEntry);
				}
			}
			
			String attrName = "attr"+j;
			aclMap.put(attrName, attrACL);
		}
	}
	
	
	private void generateACLUnifomrlyWithCircles( HashMap<String, List<ACLEntry>> aclMap, 
			HashMap<Integer, List<ACLEntry>> circlesMap )
	{
		for( int j=0; j < SearchAndUpdateDriver.numAttrs; j++ )
		{
			List<ACLEntry> attrACL  = new LinkedList<ACLEntry>();
			
			for( int k = 0; k < SearchAndUpdateDriver.numCircles; k++ )
			{
				double randVal = aclRand.nextDouble();
				
				// if less than 0.5, with half chance we pick 
				// this class for ACL
				if( randVal <= 0.5 )
				{
					List<ACLEntry> classMemberList = circlesMap.get(k);
					for( int l = 0; l < classMemberList.size(); l++ )
					{
						ACLEntry aclEntry = classMemberList.get(l);
						attrACL.add(aclEntry);
					}
				}
			}
			
			// don't want ACL to be empty
			if(attrACL.size() == 0)
			{
				int randClass = aclRand.nextInt(SearchAndUpdateDriver.numCircles);
				List<ACLEntry> classMemberList = circlesMap.get(randClass);
				for( int l = 0; l < classMemberList.size(); l++ )
				{
					ACLEntry aclEntry = classMemberList.get(l);
					attrACL.add(aclEntry);
				}
			}
			
			String attrName = "attr"+j;
			aclMap.put(attrName, attrACL);
		}
	}
	
	
	private void sendAInitMessage(int guidNum) throws Exception
	{
		UserEntry userEntry 
					= SearchAndUpdateDriver.usersVector.get(guidNum);
		
		JSONObject attrValJSON = new JSONObject();
		
		double attrDiff = SearchAndUpdateDriver.ATTR_MAX-SearchAndUpdateDriver.ATTR_MIN;
		
		for( int i=0; i<SearchAndUpdateDriver.numAttrs; i++ )
		{
			String attrName = SearchAndUpdateDriver.attrPrefix+i;
			double attrVal  = SearchAndUpdateDriver.ATTR_MIN 
					+ attrDiff * initRand.nextDouble();
			attrValJSON.put(attrName, attrVal);
		}
		
		GuidEntry myGUIDInfo = userEntry.getGuidEntry();
		String guidString = userEntry.getGuidEntry().getGuid();
		
		ExperimentUpdateReply updateRep 
					= new ExperimentUpdateReply(guidNum, guidString);
	
		
		SearchAndUpdateDriver.csClient.sendUpdateSecureWithCallback
						( guidString, myGUIDInfo, attrValJSON, -1, 
								userEntry.getACLMap(), 
								userEntry.getAnonymizedIDList(),
								updateRep, this.getCallBack() );
		
		//String userGUID = userEntry.getGuidEntry().getGuid();	
//		UpdateTask updTask = new UpdateTask(attrValJSON, userEntry, this);
//		SearchAndUpdateDriver.taskES.execute(updTask);
	}
	
	
	private void sendAInitMessageBlocking(int guidNum) throws Exception
	{
		UserEntry userEntry 
					= SearchAndUpdateDriver.usersVector.get(guidNum);
		
		JSONObject attrValJSON = new JSONObject();
		
		double attrDiff   = SearchAndUpdateDriver.ATTR_MAX-SearchAndUpdateDriver.ATTR_MIN;
		
		for( int i=0; i<SearchAndUpdateDriver.numAttrs; i++ )
		{
			String attrName = SearchAndUpdateDriver.attrPrefix+i;
			double attrVal  = SearchAndUpdateDriver.ATTR_MIN 
					+ attrDiff * initRand.nextDouble();
			attrValJSON.put(attrName, attrVal);
		}

		long start = System.currentTimeMillis();

		GuidEntry myGUIDInfo = userEntry.getGuidEntry();
		String guidString = userEntry.getGuidEntry().getGuid();


		SearchAndUpdateDriver.csClient.sendUpdateSecure( 
				guidString, myGUIDInfo, attrValJSON, -1, 
				userEntry.getACLMap(), userEntry.getAnonymizedIDList() );

		long end = System.currentTimeMillis();
		
		System.out.println("sendAInitMessageBlocking guidNum "+guidNum
				+" time "+(end-start));
	}
	
	
	public void backToBackRequestSender() throws Exception
	{
		double totalNumUsersSent = 0;
		
		while(  totalNumUsersSent < SearchAndUpdateDriver.numUsers  )
		{
			sendAInitMessageBlocking((int)totalNumUsersSent);
			totalNumUsersSent++;
			numSent++;
			assert(numSent == totalNumUsersSent);
			if(totalNumUsersSent >= SearchAndUpdateDriver.numUsers)
			{
				break;
			}		
		}
	}
	
	public void initializaRateControlledRequestSender() throws Exception
	{	
		this.startExpTime();
		
		double reqsps = SearchAndUpdateDriver.initRate;
		
		double totalNumUsersSent = 0;
		
		while(  totalNumUsersSent < SearchAndUpdateDriver.numUsers  )
		{
			for(int i=0; i<reqsps; i++ )
			{
				sendAInitMessage((int)totalNumUsersSent);
				totalNumUsersSent++;
				numSent++;
				assert(numSent == totalNumUsersSent);
				if(totalNumUsersSent >= SearchAndUpdateDriver.numUsers)
				{
					break;
				}
			}
			if(totalNumUsersSent >= SearchAndUpdateDriver.numUsers)
			{
				break;
			}
			Thread.sleep(1000);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("UserInit eventual sending rate "+sendingRate);
		
		waitForFinish();
		
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput = (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("UserInit result:Goodput "+sysThrput);
	}
		
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken) 
	{
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("UserInit reply recvd "+userGUID+" time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken)
	{
	}
	
	
	public static void main(String[] args)
	{
		
		
		
	}
}
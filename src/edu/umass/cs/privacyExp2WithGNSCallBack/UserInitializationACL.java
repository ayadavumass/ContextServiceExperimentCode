package edu.umass.cs.privacyExp2WithGNSCallBack;


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

public class UserInitializationACL extends 
									AbstractRequestSendingClass
{	
	private final Random initRand;
	private final Random aclRand;
	
	public UserInitializationACL() throws Exception
	{
		super( SearchAndUpdateDriver.INSERT_LOSS_TOLERANCE );
		initRand = new Random(SearchAndUpdateDriver.myID*100);
		aclRand  = new Random((SearchAndUpdateDriver.myID+1)*102);
		
		
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
			int guidNum = (SearchAndUpdateDriver.myID+1)*10000+i;
			
			String guidAlias = "UserGUID"+guidNum;
			InitTask currT = new InitTask(guidAlias);
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
		
		double sumDistinctGuidAllUsers  = 0.0;
		double sumAnonymizedIDsAllUsers = 0.0;
		
		// generate ACLs.
		for( int i=0; i < SearchAndUpdateDriver.numUsers; i++ )
		{
			UserEntry currUserEntry 
						= SearchAndUpdateDriver.usersVector.get(i);
			
			List<ACLEntry> distinctGuidInACLsList 
						= getTotalDistinctGuidsForAUser();
			
			// generate ACLs
			HashMap<String, List<ACLEntry>> aclMap 
									= new HashMap<String, List<ACLEntry>>();
						
			for( int j=0; j< SearchAndUpdateDriver.numAttrs; j++ )
			{
				List<ACLEntry> aclListForAttribute 
							= getACLForAnAttribute(distinctGuidInACLsList);
				
				String attrName = "attr"+j;
				aclMap.put(attrName, aclListForAttribute);
			}
			
			currUserEntry.setACLMap( aclMap );
			
			// generate anonymized IDs
			List<AnonymizedIDEntry> anonymizedIDList = 
					SearchAndUpdateDriver.csClient.computeAnonymizedIDs
					(currUserEntry.getGuidEntry(), aclMap, true);
			
			if( anonymizedIDList != null )
			{
//				System.out.println( "Number of anonymized IDs created "+
//						anonymizedIDList.size() );
			}
			currUserEntry.setAnonymizedIDList(anonymizedIDList);
			
			int numGuidsInACLs =  getDistinctGUIDsInACLs(aclMap);
			sumDistinctGuidAllUsers = sumDistinctGuidAllUsers + numGuidsInACLs;
			sumAnonymizedIDsAllUsers = sumAnonymizedIDsAllUsers + anonymizedIDList.size();
		}
		
		double avgDisNumGuidsInACLs = sumDistinctGuidAllUsers/SearchAndUpdateDriver.numUsers;
		double avgNumAnonymizedIds = sumAnonymizedIDsAllUsers/SearchAndUpdateDriver.numUsers;
		
		System.out.println("Avg num GUIDs In ACLs "+avgDisNumGuidsInACLs
				+" Avg num anonymized IDs "+avgNumAnonymizedIds);
	}
	
	private List<ACLEntry> getTotalDistinctGuidsForAUser()
	{
		HashMap<String, ACLEntry> distinctGuidMap 
							= new HashMap<String, ACLEntry>();
		
		// generate classes
		while( distinctGuidMap.size() != 
							SearchAndUpdateDriver.totalDistinctGuidsInACLs )
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
		
			distinctGuidMap.put( Utils.byteArrayToHex(guidACLMember), aclEntry );
		}
		
		List<ACLEntry> distnctGuidsList = new LinkedList<>();
		
		Iterator<String> guidIter = distinctGuidMap.keySet().iterator();
		
		while( guidIter.hasNext() )
		{
			distnctGuidsList.add(distinctGuidMap.get( guidIter.next()) );
		}
		
		assert( distnctGuidsList.size() 
					== SearchAndUpdateDriver.totalDistinctGuidsInACLs );
		
		return distnctGuidsList;
	}
	
	private List<ACLEntry> getACLForAnAttribute(List<ACLEntry> distnctGuidsList)
	{
		HashMap<String, ACLEntry> distinctGuidsInACLMap = new HashMap<String, ACLEntry>();
		
		while( distinctGuidsInACLMap.size() != SearchAndUpdateDriver.aclSize)
		{
			int randomIndex = aclRand.nextInt(distnctGuidsList.size());
			
			ACLEntry aclEntry = distnctGuidsList.get(randomIndex);
			
			String aclGUIDString = Utils.byteArrayToHex(aclEntry.getACLMemberGUID());
			
			distinctGuidsInACLMap.put(aclGUIDString, aclEntry);
		}
		
		List<ACLEntry> distinctGuidsACLList = new LinkedList<ACLEntry>();
		
		Iterator<String> guidIter = distinctGuidsInACLMap.keySet().iterator();
		
		while( guidIter.hasNext() )
		{
			distinctGuidsACLList.add
				( distinctGuidsInACLMap.get(guidIter.next()) );
		}
		
		assert(distinctGuidsACLList.size() == distinctGuidsInACLMap.size() );
		
		return distinctGuidsACLList;
	}
	
	
	private int getDistinctGUIDsInACLs(HashMap<String, 
			List<ACLEntry>> aclMap)
	{
		HashMap<String, Boolean> guidMap = new HashMap<String, Boolean>();
		
		Iterator<String> attrIter = aclMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			List<ACLEntry> aclList = aclMap.get(attrName);
			
			for( int i=0; i<aclList.size(); i++ )
			{
				ACLEntry aclEntry = aclList.get(i);
				
				String guid = Utils.byteArrayToHex(aclEntry.getACLMemberGUID());
				guidMap.put(guid, true);
			}
		}
		return guidMap.size();
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
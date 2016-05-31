package edu.umass.cs.privacyExp;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gnsclient.client.GuidEntry;

public class UserInitializationRandomACL1 extends 
										AbstractRequestSendingClass
{
	// different random generator for each variable, as using one for 
	// all of them doesn't give uniform properties.
	
	private final Random initRand;
	private final KeyPairGenerator kpg;
	private final Random aclRand;
	
	public UserInitializationRandomACL1() throws Exception
	{
		super( SearchAndUpdateDriver.INSERT_LOSS_TOLERANCE );
		initRand = new Random(SearchAndUpdateDriver.myID*100);
		aclRand  = new Random((SearchAndUpdateDriver.myID+1)*102);
		
		kpg = KeyPairGenerator.getInstance
					( ContextServiceConfig.AssymmetricEncAlgorithm );
		
		// just generate all user entries.
		generateUserEntries();
	}
	
	private void sendAInitMessage(int guidNum) throws Exception
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
		//String userGUID = userEntry.getGuidEntry().getGuid();
		
		UpdateTask updTask = new UpdateTask(attrValJSON, userEntry, this);
		SearchAndUpdateDriver.taskES.execute(updTask);
	}
	
	/**
	 * All things happen in this function are local
	 * no cs communication so no need for rate control.
	 * @throws Exception
	 */
	private void generateUserEntries() throws Exception
	{
		// generate guids
		for( int i=0; i < SearchAndUpdateDriver.numUsers; i++ )
		{
			int guidNum = i;
			
			if( SearchAndUpdateDriver.useGNS )
			{
				assert(false);
//				GuidEntry userGuidEntry = SearchAndUpdateDriver.gnsClient.guidCreate(
//						SearchAndUpdateDriver.accountGuid, SearchAndUpdateDriver.guidPrefix+guidNum);
			}
			else
			{
				String alias = SearchAndUpdateDriver.guidPrefix+guidNum;
				KeyPair kp0 = kpg.genKeyPair();
				PublicKey publicKey0 = kp0.getPublic();
				PrivateKey privateKey0 = kp0.getPrivate();
				byte[] publicKeyByteArray0 = publicKey0.getEncoded();
				
				String guid0 = Utils.convertPublicKeyToGUIDString(publicKeyByteArray0);
				GuidEntry myGUID = new GuidEntry(alias, guid0, publicKey0, privateKey0);
				
				UserEntry userEntry = new UserEntry(myGUID);
				SearchAndUpdateDriver.usersVector.add(userEntry);
			}
		}
		
		// generate ACLs.
		for( int i=0; i < SearchAndUpdateDriver.numUsers; i++ )
		{
			UserEntry currUserEntry = SearchAndUpdateDriver.usersVector.get(i);
			// there is a map to have unique 20 elements
			HashMap<String, ACLEntry> unionACLEntryMap 
								= new HashMap<String, ACLEntry>();
			
			//List<ACLEntry> unionACLEntry = new LinkedList<ACLEntry>();
			
			while( unionACLEntryMap.size() != SearchAndUpdateDriver.UNION_ACL_SIZE )
			{
				int randIndex = aclRand.nextInt(
						SearchAndUpdateDriver.usersVector.size() );
				GuidEntry randGuidEntry 
						= SearchAndUpdateDriver.usersVector.get(randIndex).getGuidEntry();
				byte[] guidACLMember = Utils.hexStringToByteArray(randGuidEntry.getGuid());
				byte[] publicKeyBytes = randGuidEntry.getPublicKey().getEncoded();
				
				ACLEntry aclEntry = new ACLEntry(guidACLMember, publicKeyBytes);
				unionACLEntryMap.put( Utils.bytArrayToHex(guidACLMember), 
						aclEntry );
				//unionACLEntry.add(aclEntry);
			}			
			
			currUserEntry.setUnionOfACLs(unionACLEntryMap);
			
			// generate ACLs by picking 10 random entries 
			// from the union of ACLs for each attribute.
			HashMap<String, List<ACLEntry>> aclMap 
								= new HashMap<String, List<ACLEntry>>();
			
			for( int j=0; j < SearchAndUpdateDriver.numAttrs; j++ )
			{
				//List<ACLEntry> attrACLList 
				//						= new LinkedList<ACLEntry>();
				String[] guidArray = new String[unionACLEntryMap.size()];
				
				guidArray = unionACLEntryMap.keySet().toArray(guidArray);
				
				HashMap<String, ACLEntry> attrACLMap 
										= new HashMap<String, ACLEntry>();
				
				while( attrACLMap.size() != SearchAndUpdateDriver.ACL_SIZE )
				{
					int randIndex = aclRand.nextInt( guidArray.length );
					ACLEntry aclEntry = unionACLEntryMap.get(guidArray[randIndex]);
					
					attrACLMap.put(Utils.bytArrayToHex(aclEntry.getACLMemberGUID()), aclEntry);
				}
				
				Iterator<String> guidIter = attrACLMap.keySet().iterator();
				List<ACLEntry> aclList = new LinkedList<ACLEntry>();
				while( guidIter.hasNext() )
				{
					String guidStr = guidIter.next();
					aclList.add(attrACLMap.get(guidStr));
				}
				
				
				String attrName = "attr"+j;
				
				aclMap.put(attrName, aclList);
			}
			currUserEntry.setACLMap( aclMap );
			
			List<AnonymizedIDEntry> anonymizedIDList = 
						SearchAndUpdateDriver.csClient.computeAnonymizedIDs
							(currUserEntry.getGuidEntry(), aclMap);
			
			if(anonymizedIDList != null)
			{
				System.out.println( "Number of anonymized IDs created "+
						anonymizedIDList.size() );
				for(int k=0; k<anonymizedIDList.size(); k++)
				{
					System.out.println("UserNum"+i+" "+
									anonymizedIDList.get(k).toString());
				}
			}
			
			currUserEntry.setAnonymizedIDList(anonymizedIDList);
		}
	}
	
	public void initializaRateControlledRequestSender() throws Exception
	{	
		this.startExpTime();
		double reqspms = SearchAndUpdateDriver.initRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double totalNumUsersSent = 0;
		
		while(  totalNumUsersSent < SearchAndUpdateDriver.numUsers  )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
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
			Thread.sleep(100);
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
}
package edu.umass.cs.privacyExp;

import java.security.KeyPairGenerator;

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

public class UserInitializationACLWithClasses extends 
												AbstractRequestSendingClass
{
	// different random generator for each variable, as using one for 
	// all of them doesn't give uniform properties.
		
	private final Random initRand;
	private final KeyPairGenerator kpg;
	private final Random aclRand;
		
	public UserInitializationACLWithClasses() throws Exception
	{
		super( SearchAndUpdateDriver.INSERT_LOSS_TOLERANCE );
		initRand = new Random(SearchAndUpdateDriver.myID*100);
		aclRand  = new Random((SearchAndUpdateDriver.myID+1)*102);
		
		kpg = KeyPairGenerator.getInstance
					( ContextServiceConfig.AssymmetricEncAlgorithm );
		
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
						= SearchAndUpdateDriver.CLASS_SIZE*SearchAndUpdateDriver.NUM_CLASSES;
			
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
				distinctGuidMap.put( Utils.bytArrayToHex(guidACLMember), 
						aclEntry );
				//unionACLEntry.add(aclEntry);
			}
			
			HashMap<Integer, List<ACLEntry>> aclClasses 
								= new HashMap<Integer, List<ACLEntry>>();
			
			Iterator<String> guidMapIter 
								= distinctGuidMap.keySet().iterator();
			
			int classnum = 0;
			int currClassSize = 0;
			
			List<ACLEntry> aclClassList = null;
			
			while( guidMapIter.hasNext() )
			{
				String guidACLMemberString = guidMapIter.next();
				ACLEntry currACLEntry = distinctGuidMap.get(guidACLMemberString);
				
				
				if( currClassSize == 0 )
				{
					aclClassList = new LinkedList<ACLEntry>();
					aclClassList.add(currACLEntry);
					currClassSize++;
					
					if( currClassSize == SearchAndUpdateDriver.CLASS_SIZE )
					{
						aclClasses.put(classnum, aclClassList);
						classnum++;
						currClassSize = 0;
					}
				}
				else
				{
					aclClassList.add(currACLEntry);
					currClassSize++;
					
					if( currClassSize == SearchAndUpdateDriver.CLASS_SIZE )
					{
						aclClasses.put(classnum, aclClassList);
						classnum++;
						currClassSize = 0;
					}
				}
			}
			currUserEntry.setACLClasses(aclClasses);
			
			
			// generate ACLs
			HashMap<String, List<ACLEntry>> aclMap 
						= new HashMap<String, List<ACLEntry>>();

			for( int j=0; j < SearchAndUpdateDriver.numAttrs; j++ )
			{
				//List<ACLEntry> attrACLList 
				//						= new LinkedList<ACLEntry>();
				List<ACLEntry> attrACL  = new LinkedList<ACLEntry>();
				
				for( int k = 0; k < SearchAndUpdateDriver.NUM_CLASSES; k++ )
				{
					double randVal = aclRand.nextDouble();
					
					// if less than 0.5, with half chance we pick 
					// this class for ACL
					if( randVal <= 0.5 )
					{
						List<ACLEntry> classMemberList = aclClasses.get(k);
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
					int randClass = aclRand.nextInt(SearchAndUpdateDriver.NUM_CLASSES);
					List<ACLEntry> classMemberList = aclClasses.get(randClass);
					for( int l = 0; l < classMemberList.size(); l++ )
					{
						ACLEntry aclEntry = classMemberList.get(l);
						attrACL.add(aclEntry);
					}
				}
				
				String attrName = "attr"+j;
				aclMap.put(attrName, attrACL);
			}
			currUserEntry.setACLMap( aclMap );
			
			
			// generate anonymized IDs
			
			List<AnonymizedIDEntry> anonymizedIDList = 
					SearchAndUpdateDriver.csClient.computeAnonymizedIDs(aclMap);
			
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
				guidString, myGUIDInfo, attrValJSON, -1, true, 
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
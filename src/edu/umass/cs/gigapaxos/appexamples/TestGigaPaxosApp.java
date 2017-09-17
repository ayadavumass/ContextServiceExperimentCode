package edu.umass.cs.gigapaxos.appexamples;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.appexamples.TestAppRequest.ResponseCodes;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.examples.AbstractReconfigurablePaxosApp;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * This is a test gigapaxos app
 * @author ayadav
 *
 */
public class TestGigaPaxosApp extends AbstractReconfigurablePaxosApp<String> implements 
							Replicable, Reconfigurable, ClientMessenger, AppRequestParserBytes
{
	private static final String DEFAULT_INIT_STATE = "";
	private boolean verbose = true;
	
	private static final boolean DELEGATE_RESPONSE_MESSAGING = true;
	
	private class AppData 
	{
		final String name;
		String state = DEFAULT_INIT_STATE;

		public AppData(String name, String state) 
		{
			this.name = name;
			this.state = state;
		}

		void setState(String state) 
		{
			this.state = state;
		}

		String getState() 
		{
			return this.state;
		}
	}
	
	private String myID; // used only for pretty printing
	private final HashMap<String, AppData> appData = new HashMap<String, AppData>();
	// only address based communication needed in app
	private SSLMessenger<?, JSONObject> messenger;
	
	/**
	 * Constructor used to create app replica via reflection. A reconfigurable
	 * app must support a constructor with a single String[] as an argument.
	 * 
	 * @param args
	 */
	public TestGigaPaxosApp(String[] args) {
	}
	
	@Override
	public void setClientMessenger(SSLMessenger<?, JSONObject> msgr) 
	{
		this.messenger = msgr;
		this.myID = msgr.getMyID().toString();
	}

	@Override
	public boolean execute(Request request, boolean doNotReplyToClient) 
	{
		if (request.toString().equals(Request.NO_OP))
			return true;
		switch ((TestAppRequest.PacketType) (request.getRequestType())) {
		case READ_REQUEST:
			return processReadRequest((TestAppRequest) request, doNotReplyToClient);
		case WRITE_REQUEST:
			return processWriteRequest((TestAppRequest) request, doNotReplyToClient);
		default:
			// everything else is an absolute no-op
			break;
		}
		return false;
	}
	
	@Override
	public boolean execute(Request request) 
	{
		return this.execute(request, false);
	}
	
	
	private boolean processReadRequest(TestAppRequest request,
			boolean doNotReplyToClient) 
	{
		if (request.getServiceName() == null)
			return true; // no-op
		if (request.isStop())
			return processStopRequest(request);
		AppData data = this.appData.get(request.getServiceName());
		if (data == null) {
			System.out.println("App-" + myID + " has no record for "
					+ request.getServiceName() + " for " + request);
			assert (request.getResponse() == null);
			return false;
		}
		assert (data != null);
		String currValue = data.getState();
		//data.setState(request.getValue());
		//this.appData.put(request.getServiceName(), data);
		if (verbose)
			System.out.println("App-" + myID + " wrote to " + data.name
					+ " with state " + data.getState());
		if (DELEGATE_RESPONSE_MESSAGING)
		{
			request.setResponse(ResponseCodes.ACK.toString() + " read of servicename "
					+ request.getServiceName()
					+ " value "+currValue);
		}
		else
			sendResponse(request, doNotReplyToClient);
		return true;
	}
	
	
	private boolean processWriteRequest(TestAppRequest request,
			boolean doNotReplyToClient) 
	{
		if (request.getServiceName() == null)
			return true; // no-op
		if (request.isStop())
			return processStopRequest(request);
		AppData data = this.appData.get(request.getServiceName());
		if (data == null) {
			System.out.println("App-" + myID + " has no record for "
					+ request.getServiceName() + " for " + request);
			assert (request.getResponse() == null);
			return false;
		}
		assert (data != null);
		data.setState(request.getValue());
		this.appData.put(request.getServiceName(), data);
		if (verbose)
			System.out.println("App-" + myID + " wrote to " + data.name
					+ " with state " + data.getState());
		if (DELEGATE_RESPONSE_MESSAGING)
		{
			request.setResponse(ResponseCodes.ACK.toString() + " write of servicename "
					+ request.getServiceName()
					+ " value "+request.getValue());
		}
		else
			sendResponse(request, doNotReplyToClient);
		return true;
	}
	
	/**
	 * This method exemplifies one way of sending responses back to the client.
	 * A cleaner way of sending a simple, single-message response back to the
	 * client is to delegate it to the replica coordinator, as exemplified below
	 * in {@link #sendResponse(AppRequest)} and supported by gigapaxos.
	 * 
	 * @param request
	 * @param doNotReplyToClient
	 */
	private void sendResponse(TestAppRequest request, boolean doNotReplyToClient) {
		assert (this.messenger != null && this.messenger.getClientMessenger() != null);
		if (this.messenger == null || doNotReplyToClient)
			return;

		InetSocketAddress sockAddr = request.getSenderAddress();
		try {
			this.messenger.getClientMessenger().sendToAddress(sockAddr,
					request.toJSONObject());
		} catch (JSONException | IOException e) {
			e.printStackTrace();
		}
	}
	
	// no-op
	private boolean processStopRequest(TestAppRequest request) {
		return true;
	}
	
	@Override
	public Request getRequest(String stringified) throws RequestParseException {
		try 
		{
			return staticGetRequest(stringified);
		} catch (JSONException je) 
		{
			System.out.println("App-" + myID + " unable to parse request " + stringified);
			throw new RequestParseException(je);
		}
	}
	
	/**
	 * We use this method also at the client, so it is static.
	 * 
	 * @param stringified
	 * @return App request
	 * @throws RequestParseException
	 * @throws JSONException
	 */
	public static Request staticGetRequest(String stringified)
			throws RequestParseException, JSONException {
		return new TestAppRequest(new JSONObject(stringified));
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return staticGetRequestTypes();
	}
	

	@Override
	public Request getRequest(byte[] arg0, NIOHeader arg1) throws RequestParseException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public String checkpoint(String name) {
		AppData data = this.appData.get(name);
		return data != null ? data.getState() : null;
	}

	@Override
	public boolean restore(String name, String state) {
		AppData data = this.appData.get(name);
		
		// if no previous state, this is a creation epoch.
		if (data == null && state != null) {
			data = new AppData(name, state);
			if (verbose)
				System.out.println(">>>App-" + myID + " creating " + name
						+ " with state " + state);
		}
		// if state==null => end of epoch
		else if (state == null) {
			if (data != null)
				if (verbose)
					System.out.println("App-" + myID + " deleting " + name
							+ " with final state " + data.state);
			this.appData.remove(name);
			assert (this.appData.get(name) == null);
		} 
		// typical reconfiguration or epoch change
		else if (data != null && state != null) {
			System.out.println("App-" + myID + " updating " + name
					+ " with state " + state);
			data.state = state;
		} 
		else
			// do nothing when data==null && state==null
			;
		
		if (state != null)
			this.appData.put(name, data);

		return true;
	}
	
	
	public String toString() {
		return TestGigaPaxosApp.class.getSimpleName();
	}
	
	/**
	 * We use this method also at the client, so it is static.
	 * 
	 * @return App request types.
	 */
	public static Set<IntegerPacketType> staticGetRequestTypes() 
	{
		Set<IntegerPacketType> copy 
			= new HashSet<IntegerPacketType>(Arrays.asList(TestAppRequest.PacketType.values()));
		return copy;
	}
	
	
	/**
	 * @param bytes
	 * @param header
	 * @return Request constructed from bytes.
	 * @throws RequestParseException
	 */
	public static Request staticGetRequest(byte[] bytes, NIOHeader header)
			throws RequestParseException {
		try {
			return staticGetRequest(new String(bytes, NIOHeader.CHARSET));
		} catch (UnsupportedEncodingException | JSONException e) {
			throw new RequestParseException(e);
		}
	}
}
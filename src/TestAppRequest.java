

import java.net.InetSocketAddress;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.utils.Util;

public class TestAppRequest extends JSONPacket implements ReconfigurableRequest,
															ReplicableRequest, ClientRequest 
{
	/**
	 * Packet type class for TestGigaPaxosApp requests.
	 */
	public enum PacketType implements IntegerPacketType 
	{
		/**
		 * Read app request.
		 */
		READ_REQUEST(501),
		/**
		 * Write app request.
		 */
		WRITE_REQUEST(502);
		

		/******************************** BEGIN static ******************************************/
		private static HashMap<Integer, PacketType> numbers = new HashMap<Integer, PacketType>();
		/* *****BEGIN static code block to ensure correct initialization ****** */
		static {
			for (PacketType type : PacketType.values()) {
				if (!PacketType.numbers.containsKey(type.number)) {
					PacketType.numbers.put(type.number, type);
				} else {
					assert (false) : "Duplicate or inconsistent enum type";
					throw new RuntimeException(
							"Duplicate or inconsistent enum type");
				}
			}
		}

		/*  *************** END static code block to ensure correct
		 * initialization ************* */
		/**
		 * @param type
		 * @return PacketType from int type.
		 */
		public static PacketType getPacketType(int type) {
			return PacketType.numbers.get(type);
		}

		/********************************** END static ******************************************/

		private final int number;

		PacketType(int t) {
			this.number = t;
		}

		@Override
		public int getInt() {
			return this.number;
		}
	}
	
	
	/**
	 * These app keys by design need not be the same as those in
	 * BasicReconfigurationPacket
	 */
	public enum Keys {
		/**
		 * 
		 */
		NAME,

		/**
		 * 
		 */
		EPOCH,

		/**
		 * 
		 */
		QID,

		/**
		 * 
		 */
		QVAL,

		/**
		 * 
		 */
		STOP,

		/**
		 * 
		 */
		COORD,

		/**
		 * 
		 */
		CSA,

		/**
		 * 
		 */
		RVAL
	};
	
	/**
	 */
	public enum ResponseCodes {
		/**
		 * 
		 */
		ACK
	};
	
	private final String name;
	private/* final */int epoch;
	private final long id;
	private final boolean stop;
	private final String value;

	private String response = null;

	private InetSocketAddress clientAddress = null;

	private boolean coordType = true;
	
	
	/**
	 * @param name
	 * @param value
	 * @param type
	 * @param stop
	 */
	public TestAppRequest(String name, String value, IntegerPacketType type,
			boolean stop) {
		this(name, 0, (int) (Math.random() * Integer.MAX_VALUE), value, type,
				stop);
	}
	
	/**
	 * @param name
	 * @param epoch
	 * @param id
	 * @param value
	 * @param type
	 * @param stop
	 */
	public TestAppRequest(String name, int epoch, long id, String value,
			IntegerPacketType type, boolean stop) 
	{
		super(type);
		this.name = name;
		this.epoch = epoch;
		this.id = id;
		this.stop = stop;
		this.value = value;
	}
	
	
	/**
	 * @param name
	 * @param id
	 * @param value
	 * @param type
	 * @param stop
	 */
	public TestAppRequest(String name, long id, String value,
			IntegerPacketType type, boolean stop) {
		this(name, 0, id, value, type, stop);
	}
	
	/**
	 * @param value
	 * @param req
	 */
	public TestAppRequest(String value, TestAppRequest req) {
		this(req.name, req.epoch, req.id, value, PacketType
				.getPacketType(req.type), req.stop);
		this.clientAddress = req.clientAddress;
	}
	
	/**
	 * @param json
	 * @throws JSONException
	 */
	public TestAppRequest(JSONObject json) throws JSONException {
		super(json);
		this.name = json.getString(Keys.NAME.toString());
		this.epoch = json.getInt(Keys.EPOCH.toString());
		this.id = json.getLong(Keys.QID.toString());
		
		// TODO : remove
		assert(json.has(Keys.STOP.toString())) : json;
		
		this.stop = json.getBoolean(Keys.STOP.toString());
		this.value = json.getString(Keys.QVAL.toString());
		this.coordType = (json.has(Keys.COORD.toString()) ? json
				.getBoolean(Keys.COORD.toString()) : false);
		/* We read from json using JSONNIOTransport convention, but there is no
		 * corresponding operation in toJSONObjectImpl(). */
		InetSocketAddress isa = MessageNIOTransport.getSenderAddress(json);
		this.clientAddress = json.has(Keys.CSA.toString()) ? Util
				.getInetSocketAddressFromString(json.getString(Keys.CSA
						.toString())) : isa;
				
		this.response = json.has(Keys.RVAL.toString()) ? json
				.getString(Keys.RVAL.toString()) : null;
	}

	@Override
	public IntegerPacketType getRequestType() 
	{
		return PacketType.getPacketType(this.type);
	}

	@Override
	public String getServiceName() 
	{
		return this.name;
	}
	
	/**
	 * @return Request value.
	 */
	public String getValue() {
		return this.value;
	}

	@Override
	public long getRequestID() 
	{
		return this.id;
	}
	
	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.NAME.toString(), this.name);
		json.put(Keys.EPOCH.toString(), this.epoch);
		json.put(Keys.QID.toString(), this.id);
		json.put(Keys.STOP.toString(), this.stop);
		json.put(Keys.QVAL.toString(), this.value);
		json.put(Keys.COORD.toString(), this.coordType);
		if (this.clientAddress != null)
			json.put(Keys.CSA.toString(), this.clientAddress.toString());
		json.putOpt(Keys.RVAL.toString(), this.response);
		return json;
	}

	@Override
	public ClientRequest getResponse() {
		if (this.response != null)
			return new TestAppRequest(this.response, this);
		else
			return null;
	}

	@Override
	public boolean needsCoordination() {
		return this.coordType;
	}

	@Override
	public int getEpochNumber() {
		return this.epoch;
	}

	@Override
	public boolean isStop() {
		return this.stop;
	}
	
	@Override
	public void setNeedsCoordination(boolean b) {
		this.coordType = b;
	}
	
	@Override
	public InetSocketAddress getClientAddress() {
		return this.clientAddress;
	}
	
	/**
	 * @return Sending client's address.
	 */
	public InetSocketAddress getSenderAddress() {
		return this.clientAddress;
	}
	
	/**
	 * @param response
	 */
	public void setResponse(String response) {
		this.response = response;
	}
	
	/**
	 * @param epoch
	 * @return New TestAppRequest with epoch changed.
	 */
	public TestAppRequest setEpoch(int epoch) {
		this.epoch = epoch;
		return this;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TestAppRequest request = new TestAppRequest("name1", 0, 0, "request1",
				TestAppRequest.PacketType.READ_REQUEST, false);
		System.out.println(request);
		try {
			TestAppRequest request2 = (new TestAppRequest(request.toJSONObject()));
			assert (request.toString().equals(request2.toString()));
		} catch (JSONException je) {
			je.printStackTrace();
		}
		System.out.println("SUCCESS");
	}
}
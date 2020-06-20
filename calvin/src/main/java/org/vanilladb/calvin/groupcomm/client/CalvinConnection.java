package org.vanilladb.calvin.groupcomm.client;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.calvin.groupcomm.ResultFromServer;
import org.vanilladb.calvin.groupcomm.SPRequest;
import org.vanilladb.comm.client.VanillaCommClient;
import org.vanilladb.comm.client.VanillaCommClientListener;
import org.vanilladb.comm.view.ProcessType;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class CalvinConnection implements VanillaCommClientListener, Runnable{
	private static Logger logger = Logger.getLogger(CalvinConnection.class
			.getName());
	
	private long currentTxNumStart;
	private int selfId;
	private VanillaCommClient client;
	private int count = 0;
	private Queue<SPRequest> spQueue = new LinkedList<SPRequest>(); 
	private Map<Long, ResultFromServer> txnToRes= new HashMap<Long, ResultFromServer>();
	private Map<Integer, Long> rteIdtoTxNum = new HashMap<Integer, Long>();
//	private Queue<ClientResponse> respQueue = new LinkedList<ClientResponse>();
//	private Queue<StoredProcedureCall> spcQueue = new LinkedList<StoredProcedureCall>();
//	private Map<Long, ClientResponse> txnRespMap = new HashMap<Long, ClientResponse>();
//	private Map<Integer, Long> rteIdtoTxNumMap = new HashMap<Integer, Long>();
	
	public CalvinConnection(int id) {
		VanillaCommClient client = new VanillaCommClient(id, this);
		this.client = client;
		this.selfId = id;
		new Thread(client).start();
		// wait for all servers to start up
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
//		client.PFD();
	}
	
	@Override
	public void onReceiveP2pMessage(ProcessType senderType, int senderId, Serializable message) {
		System.out.println("Received a P2P message from " + senderType + " " + senderId
				+ ", message: " + message.toString());
	}
	
	@Override
	public void run() {
		// periodically send batch of requests
		if (logger.isLoggable(Level.INFO))
			logger.info("start periodically send request...");

		while (true) {
			sendRequest();
			count++;
		}

	}
	
	private synchronized void sendRequest() {
		// TODO modify to SP request
		String message = String.format("Request #%d from client %d", count,
				selfId);
		SPRequest req =  null;
		if((req = spQueue.poll()) != null) {
//			System.out.println("not get spQueue");
			client.sendP2pMessage(ProcessType.SERVER, 0, req);
		}
	}

	//TODO: may need change
	//put request to queue and wait for response
	//rteId -> txnNum
	public synchronized SpResultSet callStoredProc(int rteId, int pid, Object... pars)
			throws RemoteException {
		if(!rteIdtoTxNum.containsKey(rteId)) {
			rteIdtoTxNum.put(rteId, -1L);
		}
		spQueue.add(new SPRequest(selfId, rteId, pid, pars));
		notifyAll();
		try {
			ResultFromServer rs;
			while (true) {
				Long txNum = rteIdtoTxNum.get(rteId);
				if (txnToRes.containsKey(txNum)) {
					rs = txnToRes.remove(txNum);
					break;
				}
				wait();
			}
			return (SpResultSet) rs.getResultSet();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException(e.getMessage());
		}
	}

	

//	public synchronized SpResultSet callStoredProc(int rteId, int pid,
//			Object... pars) {
//		// block the calling thread until receiving corresponding request
//		if (!rteIdtoTxNumMap.containsKey(rteId)) {
//			rteIdtoTxNumMap.put(rteId, -1L);
//		}
//		StoredProcedureCall spc = new StoredProcedureCall(myId, rteId, pid,
//				pars);
//		spcQueue.add(spc);
//		notifyAll();
//		ClientResponse cr;
//		try {
//			while (true) {
//				Long txNum = rteIdtoTxNumMap.get(rteId);
//				if (txnRespMap.containsKey(txNum)) {
//					cr = txnRespMap.remove(txNum);
//					break;
//				}
//				wait();
//			}
//			return (SpResultSet) cr.getResultSet();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//			throw new RuntimeException();
//		}
//	}

	
	
}

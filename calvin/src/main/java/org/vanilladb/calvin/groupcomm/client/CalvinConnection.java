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
	}
	
	@Override
	public synchronized void onReceiveP2pMessage(ProcessType senderType, int senderId, Serializable message) {
		ResultFromServer rs = (ResultFromServer) message;
		long txNum = rs.getTxNum();
		if(rs.getClientId() == selfId) {
			long oldTxNum = rteIdtoTxNum.get(rs.getRteId());
//			System.out.println("TxNum : "+ txNum);
//			System.out.println("old : " + oldTxNum);
			//ensure the tx order is not disturbed
			if(txNum > oldTxNum) {
//				System.out.println("push into txnToRes");
				rteIdtoTxNum.put(rs.getRteId(), txNum);
				txnToRes.put(txNum, rs);
				notifyAll();
			}
		}
//		System.out.println("Received a P2P message from " + senderType + " " + senderId
//				+ ", message: " + message.toString());
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
//		String message = String.format("Request #%d from client %d", count,
//				selfId);
		SPRequest req =  null;
		if((req = spQueue.poll()) != null) {
			client.sendP2pMessage(ProcessType.SERVER, 0, req);
		}
	}

	//TODO: may need change
	//put request to queue and wait for response
	//rteId -> txnNum
	public synchronized SpResultSet callStoredProc(int rteId, int pid, Object... pars)
			throws RemoteException {
//		System.out.println("call storeProc in calvinConnection...");
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

}

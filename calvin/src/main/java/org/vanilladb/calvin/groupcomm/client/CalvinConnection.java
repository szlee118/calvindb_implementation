package org.vanilladb.calvin.groupcomm.client;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	private Set<Long> currentTxNums;
	private int selfId;
	private VanillaCommClient client;
	private int count = 0;
//	private Queue<ClientResponse> respQueue = new LinkedList<ClientResponse>();
//	private Queue<StoredProcedureCall> spcQueue = new LinkedList<StoredProcedureCall>();
//	private Map<Long, ClientResponse> txnRespMap = new HashMap<Long, ClientResponse>();
//	private Map<Integer, Long> rteIdtoTxNumMap = new HashMap<Integer, Long>();
	
	public CalvinConnection(int id) {
		VanillaCommClient client = new VanillaCommClient(id, this);
		this.client = client;
		this.selfId = id;
		new Thread(client).start();
		currentTxNums = new HashSet<Long>();
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
			logger.info("start periodically send batched request...");

		while (true) {
			sendRequest();
			count++;
		}

	}
	
	private synchronized void sendRequest() {
		// TODO modify to SP request
		String message = String.format("Request #%d from client %d", count,
				selfId);
		client.sendP2pMessage(ProcessType.SERVER, 0, message);
	}

	//TODO: may need change
	public SpResultSet callStoredProc(int pid, Object... pars)
			throws RemoteException {
		try {
			StoredProcedure<?> sp = VanillaDb.spFactory().getStroredProcedure(pid);
			sp.prepare(pars);
			return sp.execute();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException(e.getMessage());
		}
	}

	

//	public synchronized SpResultSet callStoredProc(int rteId, int pid,
//			Object... pars) {
//		// if (testRte == -1) {
//		// testTime = System.nanoTime();
//		// testRte = rteId;
//		// }
//		// System.out.println("call proc rte:" + rteId);
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
//			// System.out.println("rte " + rteId + " recv.:");
//			// if (rteId == testRte2) {
//			// System.out.println("recv time:"
//			// + (System.nanoTime() - testTime2));
//			// testRte2 = -1;
//			// }
//			return (SpResultSet) cr.getResultSet();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//			throw new RuntimeException();
//		}
//	}

	
	
}

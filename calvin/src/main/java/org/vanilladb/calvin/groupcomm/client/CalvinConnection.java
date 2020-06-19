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

	

//	public synchronized SpResultSet callStoredProc(int pid, Object... pars) {
//		StoredProcedureCall spc = new StoredProcedureCall(myId, pid, pars);
//		StoredProcedureCall[] spcs = { spc };
//		currentTxNumStart = -1;
//		clientAppl.sendRequest(spcs);
//		currentTxNums.clear();
//		while (true) {
//			try {
//				this.wait();
//				while (respQueue.size() > 0) {
//					ClientResponse r = respQueue.poll();
//					if (currentTxNums.remove(r.getTxNum())
//							&& currentTxNums.isEmpty()) {
//						return (SpResultSet) r.getResultSet();
//					}
//				}
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//	}
//
//	public synchronized SpResultSet[] callBatchedStoredProc(int[] pids,
//			List<Object[]> pars) {
//		// StoredProcedureCall[]
//		if (pids.length != pars.size())
//			throw new IllegalArgumentException(
//					"the length of pids and paramter list should be the same");
//		// System.out.println("call batch reqeust ..");
//
//		SpResultSet[] responses = new SpResultSet[pids.length];
//		StoredProcedureCall[] spcs = new StoredProcedureCall[pids.length];
//		for (int i = 0; i < pids.length; ++i)
//			spcs[i] = new StoredProcedureCall(myId, pids[i], pars.get(i));
//
//		currentTxNumStart = -1;
//		clientAppl.sendRequest(spcs);
//		currentTxNums.clear();
//		while (true) {
//			try {
//				this.wait();
//				while (respQueue.size() > 0) {
//					ClientResponse r = respQueue.poll();
//					if (currentTxNums.remove(r.getTxNum())) {
//						// System.out.println("get resp tx:" + r.getTxNum());
//						responses[(int) (r.getTxNum() - currentTxNumStart)] = (SpResultSet) r
//								.getResultSet();
//						if (currentTxNums.isEmpty())
//							return responses;
//					}
//				}
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//	}

//	@Override
//	public synchronized void onRecvClientP2pMessage(P2pMessage p2pmsg) {
//		// notify the client thread to check the response
//		ClientResponse c = (ClientResponse) p2pmsg.getMessage();
//		if (c.getClientId() == myId && currentTxNums.contains(c.getTxNum())) {
//			respQueue.add(c);
//			notifyAll();
//		}
//	}
	
	
}

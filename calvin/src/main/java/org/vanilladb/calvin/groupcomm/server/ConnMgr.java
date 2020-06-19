package org.vanilladb.calvin.groupcomm.server;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.comm.server.VanillaCommServer;
import org.vanilladb.comm.server.VanillaCommServerListener;
import org.vanilladb.comm.view.ProcessType;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class ConnMgr implements VanillaCommServerListener{
	private static Logger logger = Logger.getLogger(ConnMgr.class
			.getName());

	private VanillaCommServer server;
	private int selfId;
	private static final BlockingQueue<Serializable> msgQueue =
			new LinkedBlockingDeque<Serializable>();
	//private BlockingQueue<TotalOrderMessage> tomQueue = new LinkedBlockingQueue<TotalOrderMessage>();

	public ConnMgr(int id) {
		this.selfId= id;
		server = new VanillaCommServer(id, this);
		new Thread(server).start();

		// wait for all servers to start up
		if (logger.isLoggable(Level.INFO))
			logger.info("wait for all servers to start up comm. module");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		VanillaDb.taskMgr().runTask(new Task() {

			@Override
			public void run() {
				while (true) {
					try {
						Serializable message = msgQueue.take();
						server.sendTotalOrderMessage(message);
						//TODO call scheduler 
//						for (int i = 0; i < tom.getMessages().length; ++i) {
//							StoredProcedureCall spc = (StoredProcedureCall) tom
//									.getMessages()[i];
//							spc.setTxNum(tom.getTotalOrderIdStart() + i);
//							Calvin.scheduler().schedule(spc);
//						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		});
	}

	@Override
	public void onServerReady() {
		if (logger.isLoggable(Level.INFO))
			logger.info("The server is ready!");
	}

	@Override
	public void onServerFailed(int failedServerId) {
		if (logger.isLoggable(Level.SEVERE))
			logger.severe("Server " + failedServerId + " failed");
	}

	@Override
	public void onReceiveP2pMessage(ProcessType senderType, int senderId, Serializable message) {
		if (senderType == ProcessType.CLIENT) {
			try {
				msgQueue.put(message);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void onReceiveTotalOrderMessage(long serialNumber, Serializable message) {
		System.out.println("Received a total order message: " + message
				+ ", serial number: " + serialNumber);
	}
	
//	public void sendClientResponse(int clientId, int rteId, long txNum,
//			SpResultSet rs) {
//		// call the communication module to send the response back to client
//		P2pMessage p2pmsg = new P2pMessage(new ClientResponse(clientId, rteId,
//				txNum, rs), clientId, ChannelType.CLIENT);
//		serverAppl.sendP2pMessage(p2pmsg);
//	}
//
//	public void callStoredProc(int pid, Object... pars) {
//		StoredProcedureCall[] spcs = { new StoredProcedureCall(-1, myId, pid, pars) };
//		serverAppl.sendTotalOrderRequest(spcs);
//	}
//
//	public void pushTupleSet(int nodeId, TupleSet reading) {
//		P2pMessage p2pmsg = new P2pMessage(reading, nodeId, ChannelType.SERVER);
//		serverAppl.sendP2pMessage(p2pmsg);
//	}

//	@Override
//	public void onRecvServerP2pMessage(P2pMessage p2pmsg) {
//		Object msg = p2pmsg.getMessage();
//		if (msg.getClass().equals(TupleSet.class)) {
//			TupleSet ts = (TupleSet) msg;
//			for (Tuple t : ts.getTupleSet()) {
//				if (VanillaDdDb.serviceType() == ServiceType.CALVIN) {
//					// TODO: Cache remote records
//					// phase 4: Collect remote reads
//					((CalvinCacheMgr)VanillaDdDb.cacheMgr()).addCacheTuple(t);
//				} else
//					throw new IllegalArgumentException(
//							"Service Type Not Found Exception");
//			}
//		} else
//			throw new IllegalArgumentException();
//	}
//
//	@Override
//	public void onRecvServerTotalOrderedMessage(TotalOrderMessage tom) {
//		try {
//			tomQueue.put(tom);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}

}

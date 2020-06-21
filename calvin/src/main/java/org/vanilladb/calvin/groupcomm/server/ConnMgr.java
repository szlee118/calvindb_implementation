package org.vanilladb.calvin.groupcomm.server;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.calvin.groupcomm.KeytoRec;
import org.vanilladb.calvin.groupcomm.KeytoRecSet;
import org.vanilladb.calvin.groupcomm.ResultFromServer;
import org.vanilladb.calvin.groupcomm.SPRequest;
import org.vanilladb.calvin.server.Calvin;
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
//	private BlockingQueue<TotalOrderMessage> tomQueue = new LinkedBlockingQueue<TotalOrderMessage>();

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
						//take msg from msgQueue and send total order message
						Serializable message = msgQueue.take();
						server.sendTotalOrderMessage(message);
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
		else if(senderType == ProcessType.SERVER) {
			//collect remote read
			
			if(message.getClass().equals(KeytoRecSet.class)) {
				KeytoRecSet krs = (KeytoRecSet) message;
				for (KeytoRec kr: krs.getTupleSet()) {
					Calvin.cacheMgr().addCacheTuple(kr);
				}
			}
			else 
				throw new IllegalArgumentException("shoul be KeytoRecSet");
		}
	}

	@Override
	public void onReceiveTotalOrderMessage(long serialNumber, Serializable message) {
//		System.out.println("Received a total order message: " + ((SPRequest)message).getRteId()
//				+ ", serial number: " + serialNumber);
		SPRequest spr = (SPRequest)message;
		spr.setTxNum(serialNumber);
		Calvin.scheduler().schedule(spr);
	}
	
	public void sendClientResponse(int clientId, int rteId, long txNum,
			SpResultSet rs) {
		// send the result back to client
		ResultFromServer res = new ResultFromServer(clientId, rteId, txNum, rs);		
		server.sendP2pMessage(ProcessType.CLIENT, clientId, res);
	}

	public void callStoredProc(int pid, Object... pars) {
		SPRequest[] spcs = { new SPRequest(-1, selfId, pid, pars) };
		List<Serializable> lspc = new ArrayList<Serializable>();
		for (SPRequest spc : spcs) {
			lspc.add(spc);
		}
		server.sendTotalOrderMessages(lspc);
	}

	public void pushTupleSet(int nodeId, KeytoRecSet reading) {
		server.sendP2pMessage(ProcessType.SERVER, nodeId, reading);
	}

}

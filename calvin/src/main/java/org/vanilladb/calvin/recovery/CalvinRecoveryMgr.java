package org.vanilladb.calvin.recovery;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.calvin.groupcomm.SPRequest;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class CalvinRecoveryMgr extends RecoveryMgr{
	private static BlockingQueue<SPRequest> spcLogQueue = new LinkedBlockingQueue<SPRequest>();
	
	private static final Object lock = new Object();
	private static final Lock spcLoggerLock = new ReentrantLock();
	private static final Condition spcLoggerCondition = spcLoggerLock
			.newCondition();
	private static long lastLoggedTxn = -1;
	
	static {
		VanillaDb.taskMgr().runTask(new Task() {
			@Override
			public void run() {
				while (true) {
					try {
						SPRequest spc = spcLogQueue.take();
						new SPRecord(spc.getTxNum(), spc
								.getClientId(), spc.getRteId(), spc.getPid(),
								spc.getPars()).writeToLog();
//						synchronized (lock) {
							try {
								spcLoggerLock.lock();
								lastLoggedTxn = spc.getTxNum();
								spcLoggerCondition.signalAll();
							} finally {
								spcLoggerLock.unlock();
							}
//						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});
	}
	
	public CalvinRecoveryMgr(long txNum) {
		super(txNum, true);
	}
	
	public static void logRequest(SPRequest spc) {
		// TODO Commented for experiment
//		spcLogQueue.add(spc);
	}
	
	@Override
	public void onTxCommit(Transaction tx) {
		 //TODO Commented for experiment
//		 if (!tx.isReadOnly()) {
//			 // synchronized (lock) {
//			 try {
//				 spcLoggerLock.lock();
//				 while (tx.getTransactionNumber() > lastLoggedTxn) {
//					 try {
//						 // spcLoggerSyncObj.wait();
//						 spcLoggerCondition.await();
//					 } catch (InterruptedException e) {
//						 e.printStackTrace();
//					 }
//				 }
//			 } finally {
//				 spcLoggerLock.unlock();
//			 }
//		 }
	}
}

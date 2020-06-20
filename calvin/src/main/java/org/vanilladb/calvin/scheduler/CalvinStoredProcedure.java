package org.vanilladb.calvin.scheduler;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.calvin.sql.RecordKey;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.storedprocedure.ManuallyAbortException;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;

public abstract class CalvinStoredProcedure<H extends StoredProcedureParamHelper>{
		private Transaction tx;
		private long txNum;
		private H paramHelper;

		// Record keys
		private List<RecordKey> readKeys = new ArrayList<RecordKey>();
		private List<RecordKey> writeKeys = new ArrayList<RecordKey>();
		private RecordKey[] readKeysForLock, writeKeysForLock;
		
		private Set<Integer> participants = new HashSet<Integer>();
		private Set<Integer> activeParticipants = new HashSet<Integer>();
		private List<RecordKey> localReadKeys = new ArrayList<RecordKey>();
		private int masterId = -1;
		private int serverId = Calvin.server_id();
		
		public CalvinStoredProcedure(long txNum, H Helper) {
			this.txNum = txNum;
			this.paramHelper = Helper;

			if (Helper == null)
				throw new NullPointerException("paramHelper should not be null");
		}

		/**
		 * Prepare the RecordKey for each record to be used in this stored
		 * procedure. Use the {@link #addReadKey(RecordKey)},
		 * {@link #addWriteKey(RecordKey)} method to add keys.
		 */
		protected abstract void prepareKeys();
		
		/**
		 * Perform the transaction logic and record the result of the transaction.
		 */
		protected abstract void executeSql();

		
		private void analyzeRWSet() {
			for (RecordKey writeKey: writeKeys) {
				int partitionId = Calvin.metaMgr().getPartition(writeKey);
				participants.add(partitionId);
				activeParticipants.add(partitionId);
				
				if (masterId == -1)
					masterId = partitionId;
			}
			
			for (RecordKey readKey: readKeys) {
				int partitionId = Calvin.metaMgr().getPartition(readKey);
				participants.add(partitionId);
				if (partitionId == serverId)
					localReadKeys.add(readKey);
					
				if (masterId == -1)
					masterId = partitionId;
			}
			if (masterId == -1) {
				masterId = 0;
				participants.add(serverId);
				activeParticipants.add(serverId);
			}
			participants.add(masterId);
			activeParticipants.add(masterId);
		}

		public void prepare(Object... pars) {
			// prepare parameters
			paramHelper.prepareParameters(pars);

			// create transaction
			boolean isReadOnly = paramHelper.isReadOnly();
			this.tx = Calvin.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
			//TODO : finish recoveryMgr  
//			this.tx.addLifecycleListener(new DdRecoveryMgr(tx
//					.getTransactionNumber()));

			prepareKeys();
			
			//1. Read/write set analysis
			analyzeRWSet();
		}
		
		//TODO : Should start from here(got to finish recoveryMgr now)
//		public void requestConservativeLocks() {
//			ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
//					.concurrencyMgr();
//
//			readKeysForLock = readKeys.toArray(new RecordKey[0]);
//			writeKeysForLock = writeKeys.toArray(new RecordKey[0]);
//
//			ccMgr.prepareSp(readKeysForLock, writeKeysForLock);
//		}
//
		public final RecordKey[] getReadSet() {
			return readKeysForLock;
		}

		public final RecordKey[] getWriteSet() {
			return writeKeysForLock;
		}
//
		public SpResultSet execute() {
//			
//			try {
//				// Get conservative locks it has asked before
//				getConservativeLocks();
//
//				// phase 2: Perform local reads
//				TupleSet ts = new TupleSet(-1);
//				CalvinCacheMgr cm = (CalvinCacheMgr) VanillaDdDb.cacheMgr();
//				for (RecordKey localReadKey : localReadKeys){
//					CachedRecord rec = cm.read(localReadKey, tx);
//					ts.addTuple(localReadKey, txNum, txNum, rec);
//				}
//				
//				// phase 3: Serve remote reads
//				ConnectionMgr connMgr = VanillaDdDb.connectionMgr();
//				if (localReadKeys.isEmpty() == false) {
//					for (int partitionId: activeParticipants)
//						if (partitionId != serverId)
//							connMgr.pushTupleSet(partitionId, ts);
//				}
//				
//				// Execute transaction
//				// phase 5: tx logic execution and applying writes
//				if (activeParticipants.contains(serverId))
//					performTransactionLogic();
//
//				// The transaction finishes normally
//				tx.commit();
//
//			} catch (Exception e) {
//				tx.rollback();
//				paramHelper.setCommitted(false);
//				e.printStackTrace();
//			} finally {
//				((CalvinCacheMgr)VanillaDdDb.cacheMgr()).cleanCachedTuples(tx);
//			}
//
//			return paramHelper.createResultSet();
			return new SpResultSet(false, null, null); //to be deleted
		} 
		
		public boolean isReadOnly() {
			return paramHelper.isReadOnly();
		}
		
		public boolean isMaster() {
			return masterId == serverId;
		}
		
		public boolean isParticipant() {
			return participants.contains(serverId);
		}
		
		protected void addReadKey(RecordKey readKey) {
			readKeys.add(readKey);
		}

		protected void addWriteKey(RecordKey writeKey) {
			writeKeys.add(writeKey);
		}
		
		protected H getParamHelper() {
			return paramHelper;
		}
		
		protected Transaction getTransaction() {
			return tx;
		}
		
		protected void abort(String Msg) {
			throw new ManuallyAbortException(Msg);
		}
//		private void getConservativeLocks() {
//			ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
//					.concurrencyMgr();
//			ccMgr.executeSp(readKeysForLock, writeKeysForLock);
//		}
		
}

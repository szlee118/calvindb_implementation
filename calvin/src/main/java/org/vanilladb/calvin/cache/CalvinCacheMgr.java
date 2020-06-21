package org.vanilladb.calvin.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.vanilladb.calvin.groupcomm.KeytoRec;
import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.calvin.sql.RecordKey;
import org.vanilladb.core.storage.tx.Transaction;

public class CalvinCacheMgr {
static Map<TupleKey, CachedRecord> tupleMap = new ConcurrentHashMap<TupleKey, CachedRecord>();
	
	public CachedRecord read(RecordKey key, Transaction tx) {
		int nodeId = Calvin.metaMgr().getPartition(key);
		if (nodeId == Calvin.server_id()) {
			return LocalRecordMgr.read(key, tx);
		} else {
			TupleKey tupleKey = new TupleKey(tx.getTransactionNumber(), key);
			
			while(!tupleMap.containsKey(tupleKey));
			
			return (CachedRecord) tupleMap.get(tupleKey);
		}
	}

	public void update(RecordKey key, CachedRecord rec, Transaction tx) {
		int nodeId = Calvin.metaMgr().getPartition(key);
		if (nodeId == Calvin.server_id())
			LocalRecordMgr.update(key, rec, tx);
	}

	public void insert(RecordKey key, CachedRecord rec, Transaction tx) {
		int nodeId = Calvin.metaMgr().getPartition(key);
		if (nodeId == Calvin.server_id())
			LocalRecordMgr.insert(key, rec, tx);
	}

	public void delete(RecordKey key, Transaction tx) {
		int nodeId = Calvin.metaMgr().getPartition(key);
		if (nodeId == Calvin.server_id())
			LocalRecordMgr.delete(key, tx);
	}
	
	public void addCacheTuple(KeytoRec tuple) {
		TupleKey tkey = new TupleKey(tuple.destTxNum, tuple.key);
		tupleMap.put(tkey, tuple.rec);
	}
	
	public void cleanCachedTuples(Transaction tx) {
		for(TupleKey tupleKey : tupleMap.keySet()) {
			if(tupleKey.getTxNum() == tx.getTransactionNumber()) {
				tupleMap.remove(tupleKey);
			}
		}
	}
}

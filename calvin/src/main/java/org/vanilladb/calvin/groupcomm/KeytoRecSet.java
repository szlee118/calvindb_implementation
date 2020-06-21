package org.vanilladb.calvin.groupcomm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.vanilladb.calvin.cache.CachedRecord;
import org.vanilladb.calvin.sql.RecordKey;

public class KeytoRecSet implements Serializable{
	private static final long serialVersionUID = 319149585140841919L;
	private List<KeytoRec> keyToRecs;
	private int sinkId;

	public KeytoRecSet(int sinkId) {
		this.keyToRecs = new ArrayList<KeytoRec>();
		this.sinkId = sinkId;
	}

	public List<KeytoRec> getTupleSet() {
		return keyToRecs;
	}

	public void addTuple(RecordKey key, long srcTxNum, long destTxNum,
			CachedRecord rec) {
		keyToRecs.add(new KeytoRec(key, srcTxNum, destTxNum, rec));
	}

	public int sinkId() {
		return sinkId;
	}
}

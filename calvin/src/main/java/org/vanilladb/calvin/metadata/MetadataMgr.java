package org.vanilladb.calvin.metadata;

import org.vanilladb.calvin.sql.RecordKey;

public class MetadataMgr {
	public final static int NUM_PARTITIONS = 1;

	/**
	 * return which partition the record should go  
	 */
	public int getPartition(RecordKey key) {
		return key.hashCode() % NUM_PARTITIONS;
	}
}

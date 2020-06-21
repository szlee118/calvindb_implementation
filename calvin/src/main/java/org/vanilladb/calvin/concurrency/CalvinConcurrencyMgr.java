package org.vanilladb.calvin.concurrency;

import java.util.HashSet;
import java.util.Set;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.calvin.sql.RecordKey;

public class CalvinConcurrencyMgr extends ConcurrencyMgr {
	protected static CalvinLockTable lockTbl = new CalvinLockTable();
	
	public CalvinConcurrencyMgr(long txNumber) {
		txNum = txNumber;
	}



	public void prepareSp(RecordKey[] readKeys, RecordKey[] writeKeys) {
		if (readKeys != null)
			for (RecordKey rt : readKeys)
				lockTbl.requestLock(rt, txNum);

		if (writeKeys != null)
			for (RecordKey wt : writeKeys)
				lockTbl.requestLock(wt, txNum);
	}

	public void executeSp(RecordKey[] readKeys, RecordKey[] writeKeys) {

		if (writeKeys != null)
			for (RecordKey k : writeKeys) {
				lockTbl.xLock(k, txNum);
			}

		if (readKeys != null)
			for (RecordKey k : readKeys) {
				lockTbl.sLock(k, txNum);
			}
	}



	public void onTxCommit(Transaction tx) {
		lockTbl.releaseAll(txNum,false);
	}

	public void onTxRollback(Transaction tx) {
		lockTbl.releaseAll(txNum,false);
	}

	public void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	@Override
	public void modifyFile(String fileName) {
		// do nothing
	}

	@Override
	public void readFile(String fileName) {
		// do nothing
	}

	@Override
	public void modifyBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void readBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void insertBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void modifyIndex(String dataFileName) {
		// lockTbl.ixLock(dataFileName, txNum);
	}

	@Override
	public void readIndex(String dataFileName) {
		// lockTbl.isLock(dataFileName, txNum);
	}

	/*
	 * Methods for B-Tree index locking
	 */
	private Set<BlockId> readIndexBlks = new HashSet<BlockId>();
	private Set<BlockId> writtenIndexBlks = new HashSet<BlockId>();

	/**
	 * Sets lock on the leaf block for update.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void modifyLeafBlock(BlockId blk) {
		lockTbl.xLock(blk, txNum);
		writtenIndexBlks.add(blk);
	}

	/**
	 * Sets lock on the leaf block for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void readLeafBlock(BlockId blk) {
		lockTbl.sLock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Sets exclusive lock on the directory block when crabbing down for
	 * modification.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForModification(BlockId blk) {
		lockTbl.xLock(blk, txNum);
		writtenIndexBlks.add(blk);
	}

	/**
	 * Sets shared lock on the directory block when crabbing down for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForRead(BlockId blk) {
		lockTbl.sLock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Releases exclusive locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForModification(BlockId blk) {
		lockTbl.release(blk, txNum,
				4/*X_LOCK*/);
		writtenIndexBlks.remove(blk);
	}

	/**
	 * Releases shared locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForRead(BlockId blk) {
		lockTbl.release(blk, txNum,
				2/*S_LOCK*/);
		readIndexBlks.remove(blk);
	}


	public void lockRecordFileHeader(BlockId blk) {
		lockTbl.xLock(blk, txNum);
	}

	public void releaseRecordFileHeader(BlockId blk) {
		lockTbl.release(blk, txNum,
				4/*X_LOCK*/);
	}

	@Override
	public void modifyRecord(RecordId recId) {
		// do nothing

	}

	@Override
	public void readRecord(RecordId recId) {
		// do nothing

	}
}
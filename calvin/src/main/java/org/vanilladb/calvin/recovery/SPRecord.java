package org.vanilladb.calvin.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.LinkedList;
import java.util.List;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.LogRecord;

public class SPRecord implements CalvinLogRecord{
	private long txNum;
	private int cid, rteId, pid;
	private Object[] pars;
	private LogSeqNum lsn;
	
	/**
	 * Creates a new SP log record for the specified transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 */
	public SPRecord(long txNum, int cid, int rteId, int pid,
			Object... pars) {
		this.txNum = txNum;
		this.cid = cid;
		this.pid = pid;
		this.rteId = rteId;
		this.pars = pars;
		this.lsn = null;
	}

	/**
	 * Creates a log record by reading one other value from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public SPRecord(BasicLogRecord rec) {
		txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		cid = (Integer) rec.nextVal(INTEGER).asJavaVal();
		rteId = (Integer) rec.nextVal(INTEGER).asJavaVal();
		pid = (Integer) rec.nextVal(INTEGER).asJavaVal();
		lsn = rec.getLSN();
	}

	/**
	 * Writes a commit record to the log. This log record contains the
	 * {@link LogRecord#OP_COMMIT} operator ID, followed by the transaction ID.
	 * 
	 * @return the LSN of the log record
	 */
	@Override
	public LogSeqNum writeToLog() {
		List<Constant> rec = buildRecord();
		return calvinLogMgr.append(rec.toArray(new Constant[rec.size()]));
	}

	@Override
	public int op() {
		return OP_COMMIT;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	@Override
	public void undo(Transaction tx) {
		// do nothing now
	}

	/**
	 * Replay the SP request in log
	 */
	@Override
	public void redo(Transaction tx) {
		//TODO: replay all SP request
		
	}

	@Override
	public String toString() {
		return "<SP_REQUEST " + txNum + " " + pid + " " + cid + " >";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		return rec;
	}
	@Override
	public LogSeqNum getLSN() {
		return lsn;
	}
	
}

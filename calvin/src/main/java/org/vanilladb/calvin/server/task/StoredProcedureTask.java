package org.vanilladb.calvin.server.task;

import org.vanilladb.core.server.task.Task;
import org.vanilladb.calvin.scheduler.CalvinStoredProcedure;
import org.vanilladb.calvin.sql.RecordKey;

public abstract class StoredProcedureTask extends Task {
	protected CalvinStoredProcedure sp;
	protected int cid;
	protected int rteId;
	protected long txNum;

	public StoredProcedureTask(int cid, int rteId, long txNum,
			CalvinStoredProcedure sp) {
		this.txNum = txNum;
		this.cid = cid;
		this.rteId = rteId;
		this.sp = sp;
	}

	public abstract void run();

	public RecordKey[] getReadSet() {
		return sp.getReadSet();
	}

	public RecordKey[] getWriteSet() {
		return sp.getWriteSet();
	}

	public long getTxNum() {
		return txNum;
	}
}

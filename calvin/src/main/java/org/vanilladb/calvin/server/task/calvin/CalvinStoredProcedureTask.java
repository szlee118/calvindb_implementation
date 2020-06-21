package org.vanilladb.calvin.server.task.calvin;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.calvin.scheduler.CalvinStoredProcedure;
import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.calvin.server.task.StoredProcedureTask;

public class CalvinStoredProcedureTask extends StoredProcedureTask {
	
	private CalvinStoredProcedure<?> csp;
	
	public CalvinStoredProcedureTask(int cid, int rteId, long txNum,
			CalvinStoredProcedure sp) {
		super(cid, rteId, txNum, sp);
		
		csp = (CalvinStoredProcedure<?>) sp;
	}

	public void run() {
		SpResultSet rs = sp.execute();
		
		if (sp.isMaster())
			Calvin.connMgr().sendClientResponse(cid, rteId, txNum, rs);
	}
	
	public void lockConservatively() {
		csp.requestConservativeLocks();
	}
}

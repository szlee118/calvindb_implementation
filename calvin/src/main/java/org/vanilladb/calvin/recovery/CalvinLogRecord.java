package org.vanilladb.calvin.recovery;

import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.core.storage.tx.recovery.LogRecord;

public interface CalvinLogRecord extends LogRecord{
	
	static final int OP_SP_REQIEST = -1911;
	static CalvinLogMgr calvinLogMgr = Calvin.calvinLogMgr();
	
}

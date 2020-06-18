package org.vanilladb.calvin.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.query.planner.Planner;
import org.vanilladb.core.query.planner.QueryPlanner;
import org.vanilladb.core.query.planner.UpdatePlanner;
import org.vanilladb.core.query.planner.index.IndexUpdatePlanner;
import org.vanilladb.core.query.planner.opt.HeuristicQueryPlanner;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.TaskMgr;
import org.vanilladb.core.sql.storedprocedure.SampleStoredProcedureFactory;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureFactory;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.log.LogMgr;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.statistics.StatMgr;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionMgr;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.util.Profiler;

/**
 * The class that provides system-wide static global values. These values must
 * be initialized by the method {@link #init(String) init} before use. The
 * methods {@link #initFileMgr(String) initFileMgr},
 * {@link #initFileAndLogMgr(String) initFileAndLogMgr},
 * {@link #initTaskMgr() initTaskMgr},
 * {@link #initTxMgr() initTxMgr},
 * {@link #initCatalogMgr(boolean, Transaction) initCatalogMgr},
 * {@link #initStatMgr(Transaction) initStatMgr}, and
 * {@link #initCheckpointingTask() initCheckpointingTask} provide limited
 * initialization, and are useful for debugging purposes.
 */
public class Calvin extends VanillaDb{

	// Logger
	private static Logger logger = Logger.getLogger(VanillaDb.class.getName());
	
	private static int nodeId;

	public static void init(String dirName, int id) {
		nodeId = id;

		if (logger.isLoggable(Level.INFO))
			logger.info("Calvin initializing...");

		// initialize core modules
		VanillaDb.init(dirName);
		
		// initialize DD modules
//		initCacheMgr();
//		initPartitionMetaMgr();
//		initScheduler();
//		initConnectionMgr(myNodeId);
//		initDdLogMgr();
	}
	
	public static void init(String dirName, int id, StoredProcedureFactory factory) {
		nodeId = id;

		if (logger.isLoggable(Level.INFO))
			logger.info("Calvin initializing...");

		// initialize core modules
		VanillaDb.init(dirName, factory);
		
		// initialize DD modules
//		initCacheMgr();
//		initPartitionMetaMgr();
//		initScheduler();
//		initConnectionMgr(myNodeId);
//		initDdLogMgr();
	}
}

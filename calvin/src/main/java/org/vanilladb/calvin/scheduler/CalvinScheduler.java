package org.vanilladb.calvin.scheduler;
import java.util.concurrent.BlockingQueue;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.calvin.server.task.calvin.CalvinStoredProcedureTask;

import java.util.concurrent.LinkedBlockingQueue;
import org.vanilladb.calvin.groupcomm.SPRequest;
import org.vanilladb.calvin.recovery.CalvinRecoveryMgr;
import org.vanilladb.core.server.VanillaDb;

/*public class scheduler {
	//analyze each request (metadata) 
	//log each request (recovery)
	//generate execution plan (sql)
	//register lock (concurrency)
	//create sp instance (stored procedure)
	//dispatch instance to a thread 
}*/
public class CalvinScheduler extends Task{
	private static Class<?> FACTORY_CLASS=null;

	private CalvinStoredProcedureFactory factory;
	private BlockingQueue<SPRequest> spcQueue = new LinkedBlockingQueue<SPRequest>();
	
	static {
		try {
			FACTORY_CLASS = Class.forName("org.vanilladb.bench.server.procedure.micro.MicrobenchStoredProcFactory");
			if (FACTORY_CLASS == null)
				throw new RuntimeException("Factory property is empty");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public CalvinScheduler() {
		try {
			factory = (CalvinStoredProcedureFactory) FACTORY_CLASS.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	public void schedule(SPRequest... calls) {
		try {
			for (int i = 0; i < calls.length; i++) {
				spcQueue.put(calls[i]);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		while (true) {
			try {
				// retrieve stored procedure call
				SPRequest call = spcQueue.take();
				if (call.isNoOpStoredProcCall())
					continue;

				// create store procedure and prepare
				CalvinStoredProcedure sp = factory.getStoredProcedure(
						call.getPid(), call.getTxNum());
				sp.prepare(call.getPars());

				// log request
				if (!sp.isReadOnly())
			    		CalvinRecoveryMgr.logRequest(call);

				if (sp.isParticipant()) {
					// create a new task for multi-thread
					CalvinStoredProcedureTask spt = new CalvinStoredProcedureTask(
							call.getClientId(), call.getRteId(), call.getTxNum(),
							sp);
					
					// perform conservative locking
					spt.lockConservatively();

					// hand over to a thread to run the task
					VanillaDb.taskMgr().runTask(spt);
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}
}

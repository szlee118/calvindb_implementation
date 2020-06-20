package org.vanilladb.bench.server.procedure;

import org.vanilladb.bench.ControlTransactionType;
import org.vanilladb.calvin.scheduler.CalvinStoredProcedure;
import org.vanilladb.calvin.scheduler.CalvinStoredProcedureFactory;

public class BasicStoredProcFactory implements CalvinStoredProcedureFactory {
	
	private CalvinStoredProcedureFactory underlayerFactory;
	
	public BasicStoredProcFactory(CalvinStoredProcedureFactory underlayerFactory) {
		this.underlayerFactory = underlayerFactory;
	}

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		ControlTransactionType txnType = ControlTransactionType.fromProcedureId(pid);
		if (txnType != null) {
			switch (txnType) {
			case START_PROFILING:
				return new StartProfilingProc(txNum);
			case STOP_PROFILING:
				return new StopProfilingProc(txNum);
			}
		}
		
		return underlayerFactory.getStoredProcedure(pid, txNum);
	}


}

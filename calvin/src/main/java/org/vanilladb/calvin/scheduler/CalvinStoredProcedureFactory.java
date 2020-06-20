package org.vanilladb.calvin.scheduler;


public interface CalvinStoredProcedureFactory  {
	
	CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum);
	
}
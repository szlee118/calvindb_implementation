package org.vanilladb.calvin.groupcomm;

import java.io.Serializable;


/**
 * Use for the Store procedure Call from client
 * client will send this to connMgr in server
 * 
 */
public class SPRequest implements Serializable{
	public static int PID_NO_OPERATION = Integer.MIN_VALUE;

	private static final long serialVersionUID = 9487L;

	private Object[] pars;

	private long txNum = -1;

	private int clientId, pid = PID_NO_OPERATION, rteId = -1;

	public static SPRequest getNoOpStoredProcCall(int clienId) {
		return new SPRequest(clienId);
	}

	SPRequest(int clienId) {
		this.clientId = clienId;
	}

	public SPRequest(int clienId, int pid, Object... pars) {
		this.clientId = clienId;
		this.pid = pid;
		this.pars = pars;
	}

	public SPRequest(int clienId, int rteid, int pid, Object... pars) {
		this.clientId = clienId;
		this.rteId = rteid;
		this.pid = pid;
		this.pars = pars;
	}

	public Object[] getPars() {
		return pars;
	}

	public long getTxNum() {
		return txNum;
	}

	public void setTxNum(long txNum) {
		this.txNum = txNum;
	}

	public int getClientId() {
		return clientId;
	}

	public int getRteId() {
		return rteId;
	}

	public int getPid() {
		return pid;
	}

	public void setClientId(int clientId) {
		this.clientId = clientId;
	}

	public boolean isNoOpStoredProcCall() {
		return pid == PID_NO_OPERATION;
	}
}

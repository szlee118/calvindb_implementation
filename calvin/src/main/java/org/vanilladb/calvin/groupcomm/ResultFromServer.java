package org.vanilladb.calvin.groupcomm;

import java.io.Serializable;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;

/**
 * Result from Server after execute StoreProcedure
 * Should be send from server to client 
 *
 */
public class ResultFromServer implements Serializable{

	private static final long serialVersionUID = 331L;

	public static final int COMMITTED = 0, ROLLED_BACK = 1;

	private long txNum;

	private int clientId, rteId;

	private SpResultSet result;

	public ResultFromServer(int clientId, int rteId, long txNum, SpResultSet result) {
		this.txNum = txNum;
		this.clientId = clientId;
		this.rteId = rteId;
		this.result = result;
	}

	public long getTxNum() {
		return txNum;
	}

	public SpResultSet getResultSet() {
		return result;
	}

	public int getClientId() {
		return clientId;
	}

	public void setClientId(int clientId) {
		this.clientId = clientId;
	}

	public int getRteId() {
		return rteId;
	}

	public void setRteId(int rteId) {
		this.rteId = rteId;
	}
}

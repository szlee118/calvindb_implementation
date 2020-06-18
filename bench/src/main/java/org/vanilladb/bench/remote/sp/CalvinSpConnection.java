package org.vanilladb.bench.remote.sp;

import java.sql.Connection;
import java.sql.SQLException;

import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.calvin.groupcomm.client.CalvinConnection;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class CalvinSpConnection implements SutConnection{
	private CalvinConnection conn;
	private int rteId;
	
	public CalvinSpConnection(CalvinConnection conn, int rteId) {
		this.conn = conn;
		this.rteId = rteId;
	}

	@Override
	public SutResultSet callStoredProc(int pid, Object... pars)
			throws SQLException {
		try {
			SpResultSet r = conn.callStoredProc(rteId, pid, pars);
			return new CalvinSpResultSet(r);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		}
	}

	@Override
	public Connection toJdbcConnection() {
		throw new RuntimeException("cannot convert a stored procedure connection to a JDBC connection");
	}
}

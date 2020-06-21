package org.vanilladb.bench.remote.sp;

import java.sql.SQLException;

import org.vanilladb.bench.App;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.calvin.groupcomm.client.CalvinConnection;
import org.vanilladb.calvin.groupcomm.client.CalvinDriver;

public class CalvinSpDriver implements SutDriver{
	private static final CalvinConnection conn; 
	
	static {
		CalvinDriver driver = new CalvinDriver(App.nodeId);
		conn = driver.init();
	}
	
	
	@Override
	public SutConnection connectToSut(Object... args) throws SQLException {
		try {
			return new CalvinSpConnection(conn, (Integer) args[0]);
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new SQLException(e);
		}
	}
	
}

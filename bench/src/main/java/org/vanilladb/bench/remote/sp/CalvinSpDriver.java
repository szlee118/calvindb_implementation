package org.vanilladb.bench.remote.sp;

import java.sql.SQLException;

import org.vanilladb.bench.App;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.calvin.groupcomm.client.CalvinDriver;

public class CalvinSpDriver implements SutDriver{
	private static final CalvinDriver driver = new CalvinDriver(App.nodeId);
	
	@Override
	public SutConnection connectToSut(Object... args) throws SQLException {
		try {
			return new CalvinSpConnection(driver.init(), (Integer) args[0]);
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new SQLException(e);
		}
	}
	
}

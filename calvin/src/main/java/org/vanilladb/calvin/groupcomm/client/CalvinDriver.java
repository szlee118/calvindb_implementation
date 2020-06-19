package org.vanilladb.calvin.groupcomm.client;



public class CalvinDriver {
	private int myId;

	public CalvinDriver(int id) {
		myId = id;
	}

	public CalvinConnection init() {
		CalvinConnection conn = new CalvinConnection(myId);
		new Thread(null, conn, "ConnMgr").start();
		return conn;
	}
}

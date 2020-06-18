package org.vanilladb.calvin.groupcomm.client;

public class CalvinDriver {
	private int myId;

	public CalvinDriver(int id) {
		myId = id;
	}

	public CalvinConnection init() {
		return new CalvinConnection(myId);
	}
}

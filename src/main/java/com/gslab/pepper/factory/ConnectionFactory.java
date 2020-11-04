package com.gslab.pepper.factory;

public class ConnectionFactory {
	private static ConnectionFactory instance;

	public static ConnectionFactory getInstance() {
		if (instance == null) {
			instance = new ConnectionFactory();
		}
		return instance;
	}
}

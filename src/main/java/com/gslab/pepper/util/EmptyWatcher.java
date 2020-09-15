package com.gslab.pepper.util;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class EmptyWatcher implements Watcher{

	@Override
	public void process(WatchedEvent event) {
		
	}
	

}

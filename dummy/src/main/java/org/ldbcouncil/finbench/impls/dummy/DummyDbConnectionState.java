package org.ldbcouncil.finbench.impls.dummy;

import org.ldbcouncil.finbench.driver.DbConnectionState;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

public class DummyDbConnectionState extends DbConnectionState {

	public DummyDbConnectionState() {
	}

	@Override
	public void close() throws IOException {
	}

}

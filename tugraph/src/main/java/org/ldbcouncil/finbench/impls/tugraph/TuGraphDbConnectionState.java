package org.ldbcouncil.finbench.impls.tugraph;

import com.antgroup.tugraph.TuGraphDbRpcClient;
import org.ldbcouncil.finbench.driver.DbConnectionState;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

public class TuGraphDbConnectionState extends DbConnectionState {

    private String uri;
    private String user;
    private String pass;
    private LinkedList<TuGraphDbRpcClient> clientPool;
    private TuGraphDbRpcClient client;

    public TuGraphDbConnectionState(Map<String, String> properties) throws IOException {
        uri = properties.get("uri");
        user = properties.get("user");
        pass = properties.get("pass");
        clientPool = new LinkedList<>();
        client = new TuGraphDbRpcClient(uri, user, pass);
    }

    public synchronized TuGraphDbRpcClient popClient() throws IOException {
        if (clientPool.isEmpty()) {
            clientPool.add(new TuGraphDbRpcClient(uri, user, pass));
        }
        return clientPool.pop();
    }

    public synchronized void pushClient(TuGraphDbRpcClient client) {
        clientPool.push(client);
    }

    @Override
    public void close() throws IOException {
        while (!clientPool.isEmpty()) {
            clientPool.pop();
        }
        client.logout();
    }

}

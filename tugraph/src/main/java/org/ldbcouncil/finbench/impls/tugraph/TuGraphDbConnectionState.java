package org.ldbcouncil.finbench.impls.tugraph;

import com.antgroup.tugraph.TuGraphRpcClient;
import org.ldbcouncil.finbench.driver.DbConnectionState;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

public class TuGraphDbConnectionState extends DbConnectionState {

    private String uri;
    private String user;
    private String pass;
    private LinkedList<TuGraphRpcClient> clientPool;
    private TuGraphRpcClient client;

    public TuGraphDbConnectionState(Map<String, String> properties) throws IOException {
        uri = properties.get("uri");
        user = properties.get("user");
        pass = properties.get("pass");
        clientPool = new LinkedList<>();
        client = new TuGraphRpcClient(uri, user, pass);
    }

    public synchronized TuGraphRpcClient popClient() throws IOException {
        if (clientPool.isEmpty()) {
            clientPool.add(new TuGraphRpcClient(uri, user, pass));
        }
        return clientPool.pop();
    }

    public synchronized void pushClient(TuGraphRpcClient client) {
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

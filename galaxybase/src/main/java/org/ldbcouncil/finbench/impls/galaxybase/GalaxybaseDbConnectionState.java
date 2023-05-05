package org.ldbcouncil.finbench.impls.galaxybase;


import com.galaxybase.client.driver.Galaxybase;
import com.galaxybase.client.driver.Graph;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbConnectionState;

public class GalaxybaseDbConnectionState extends DbConnectionState {
    static Logger logger = LogManager.getLogger("GalaxybaseDbConnectionState");
    protected Graph graph;

    public GalaxybaseDbConnectionState(Map<String, String> properties) {
        String endPoint = properties.get("endpoint");
        String user = properties.get("user");
        String password = properties.get("password");
        String graphName = properties.get("graphName");
        int threadCount = Integer.parseInt(properties.get("threadCount"));
        graph = Galaxybase.driver(endPoint, graphName, user, password, threadCount);
    }

    public Graph getGraph() {
        return graph;
    }

    @Override
    public void close() {
        try {
            graph.close();
        } catch (Exception e) {
            logger.error("Galaxybase closed", e);
        }
    }
}

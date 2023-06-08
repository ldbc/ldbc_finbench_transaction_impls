package org.ldbcouncil.finbench.impls.galaxybase;


import com.graphdbapi.driver.Graph;
import com.graphdbapi.driver.GraphDb;
import com.graphdbapi.driver.v1.Driver;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.impls.common.BaseDbConnectionState;

public class GalaxybaseDbConnectionState extends BaseDbConnectionState<GalaxybaseQueryStore> {
    static Logger logger = LogManager.getLogger("GalaxybaseDbConnectionState");
    private final Driver driver;
    protected Graph graph;

    public GalaxybaseDbConnectionState(Map<String, String> properties, GalaxybaseQueryStore store) {
        super(properties, store);
        String endPoint = properties.get("endpoint");
        String user = properties.get("user");
        String password = properties.get("password");
        String graphName = properties.get("graphName");
        driver = GraphDb.connect(endPoint, user, password);
        graph = GraphDb.driver(driver, graphName);
    }

    public Graph getGraph() {
        return graph;
    }

    @Override
    public void close() {
        try {
            driver.close();
        } catch (Exception e) {
            logger.error("Galaxybase closed", e);
        }
    }
}

package org.ldbcouncil.finbench.impls.gpstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbConnectionState;
import org.pkumod.gpstore.driver.*;
import org.pkumod.gpstore.log.dialect.console.ConsoleLog;
import org.pkumod.gpstore.log.level.LogLevel;

import java.io.IOException;
import java.util.Map;
import java.util.TimeZone;

public class GpstoreDbConnectionState extends DbConnectionState {

    static Logger logger = LogManager.getLogger(GpstoreDatabase.class);
    protected final Driver driver;

    protected final SessionConfig config;

    protected GpstoreQueryStore queryStore;

    public GpstoreDbConnectionState(Map<String, String> properties, GpstoreQueryStore queryStore) {
        this.queryStore = queryStore;
        final String url = properties.get("url");
        final String username = properties.get("username");
        final String password = properties.get("password");
        final String dbname = properties.get("dbname");
        config = SessionConfig.withGrpc(dbname);
        // set driver log level
        ConsoleLog.setLevel(LogLevel.DEBUG);
        driver = GpstoreDatabase.Driver(url, AuthTokens.basic(username, password));
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    public GpstoreQueryStore getQueryStore() {
        return queryStore;
    }

    public Session getSession() {
        return driver.session(config);
    }

    @Override
    public void close() throws IOException {
        try {
            driver.close();
        } catch (Exception e) {
            logger.error("Error closing GPStore", e);
            throw new RuntimeException(e);
        }
    }
}

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

    private final String username;

    private final String password;

    private final String dbName;

    public GpstoreDbConnectionState(Map<String, String> properties, GpstoreQueryStore queryStore) {
        this.queryStore = queryStore;
        final String url = properties.get("url");
        username = properties.get("username");
        password = properties.get("password");
        dbName = properties.get("dbname");
        config = SessionConfig.withGrpc(dbName);
        // set driver log level
        ConsoleLog.setLevel(LogLevel.DEBUG);
        driver = GpstoreDatabase.Driver(url, AuthTokens.basic(username, password));
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    public GpstoreQueryStore getQueryStore() {
        return queryStore;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDbName() {
        return dbName;
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

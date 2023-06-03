package org.ldbcouncil.finbench.impls.ultipa;

import com.ultipa.sdk.connect.Connection;
import com.ultipa.sdk.connect.DefaultConnection;
import com.ultipa.sdk.connect.conf.UltipaConfiguration;
import com.ultipa.sdk.connect.driver.DataSource;
import com.ultipa.sdk.connect.driver.UltipaClientDriver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbConnectionState;

import java.io.IOException;
import java.util.Map;

public class UltipaDbConnectionState extends DbConnectionState {

    static Logger logger = LogManager.getLogger("UltipaDbConnectionState");

    protected UltipaClientDriver driver;

    protected Connection conn;

    protected UltipaQueryStore queryStore;

    public UltipaDbConnectionState(Map<String, String> properties,UltipaQueryStore queryStore) {
        this.queryStore = queryStore;

        UltipaConfiguration configuration = UltipaConfiguration.config()
                .hosts(properties.get("ultipa.grpc.pool.conn.url"))
                .username(properties.get("ultipa.grpc.pool.conn.username"))
                .password(properties.get("ultipa.grpc.pool.conn.password"))
                .timeout(Integer.valueOf(properties.get("ultipa.grpc.pool.conn.timeout")));
        //configuration.getPoolConfig().setMaxTotal(1);
        DataSource dataSource = new DataSource();
        dataSource.setUltipaConfiguration(configuration);
        driver = new UltipaClientDriver(dataSource);
        conn = new DefaultConnection(driver, properties.get("ultipa.grpc.pool.conn.graph"));
    }

    public UltipaQueryStore getQueryStore(){
        return queryStore;
    }

    public Connection getConn(){
        return conn;
    }

    @Override
    public void close() throws IOException {
        try {
            driver.close();
        } catch (Exception e) {
            logger.error("Ultipa closed", e);
        }
    }
}

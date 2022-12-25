package org.ldbcouncil.finbench.impls.common.db;

import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.impls.common.BaseDbConnectionState;
import org.ldbcouncil.finbench.impls.common.QueryStore;

import java.io.IOException;

public abstract class BaseDb<TQueryStore extends QueryStore> extends Db {

    protected BaseDbConnectionState<TQueryStore> dcs;

    @Override
    protected void onClose() throws IOException {
        dcs.close();
    }

    @Override
    protected BaseDbConnectionState getConnectionState() {
        return dcs;
    }
    
}

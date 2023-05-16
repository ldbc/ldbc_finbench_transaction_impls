package org.ldbcouncil.finbench.impls.galaxybase;

import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.impls.common.QueryStore;

public class GalaxybaseQueryStore extends QueryStore {

    public GalaxybaseQueryStore(String path) throws DbException {
        super(path, ".cypher");
    }
}

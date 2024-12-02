package org.ldbcouncil.finbench.impls.gpstore;

import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.impls.common.QueryStore;

public class GpstoreQueryStore extends QueryStore {

    public GpstoreQueryStore(String path) throws DbException {
        super(path, ".cypher");
    }


}

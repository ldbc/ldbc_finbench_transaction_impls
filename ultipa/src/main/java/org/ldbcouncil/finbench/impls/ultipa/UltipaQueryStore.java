package org.ldbcouncil.finbench.impls.ultipa;

import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.impls.common.QueryStore;

public class UltipaQueryStore extends QueryStore {

    public UltipaQueryStore(String path)  throws DbException {
        super(path, ".uql");
    }


}

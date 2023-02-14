package org.ldbcouncil.finbench.impls.common.transaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.ldbcouncil.finbench.driver.*;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcFinBenchTransactionWorkload;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public abstract class TransactionTest<D extends Db> {
    protected final D db;
    protected final Workload workload;
    protected final int LIMIT = 100;

    public TransactionTest(D db) {
        this.db = db;
        this.workload = new LdbcFinBenchTransactionWorkload();
    }

    protected abstract Map<String, String> getProperties();

    @Before
    public void init() throws DbException {
        Map<Integer, Class<? extends Operation>> mapping = workload.operationTypeToClassMapping();
        db.init(getProperties(), null, mapping);
    }

    @After
    public void cleanup() throws IOException {
        db.close();
    }

    protected void run(D db, Operation op) throws DbException {
        OperationHandlerRunnableContext handler = db.getOperationHandlerRunnableContext(op);
        ResultReporter reporter = new ResultReporter.SimpleResultReporter(null);
        handler.operationHandler().executeOperation(op, handler.dbConnectionState(), reporter);
        handler.cleanup();
    }


    @Test
    public void testQuery1() throws Exception {
        run(db, new ComplexRead1(30786325579101L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }
}


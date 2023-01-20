package org.ldbcouncil.finbench.impls.common.transaction;

import org.junit.Test;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcFinBenchTransactionWorkload;
import org.ldbcouncil.finbench.impls.common.FinBenchTest;

import java.util.Calendar;
import java.util.Date;

public abstract class TransactionTest<D extends Db> extends FinBenchTest<D> {

    public TransactionTest(D db) {
        super(db, new LdbcFinBenchTransactionWorkload());
    }

    @Test
    public void testQuery1() throws Exception {
        run(db, new ComplexRead1(30786325579101L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }
}


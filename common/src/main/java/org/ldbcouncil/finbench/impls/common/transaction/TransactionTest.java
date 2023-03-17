package org.ldbcouncil.finbench.impls.common.transaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.ldbcouncil.finbench.driver.*;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcFinBenchTransactionWorkload;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;

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
    public void testComplexRead1() throws Exception {
        run(db, new ComplexRead1(30786325579101L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }

    @Test
    public void testComplexRead2() throws Exception {
        run(db, new ComplexRead2(62042209056869L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }

    @Test
    public void testComplexRead3() throws Exception {
        run(db, new ComplexRead3(23727037716838L, 97461814013107L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }

    @Test
    public void testComplexRead4() throws Exception {
        run(db, new ComplexRead4(39947851855474L, 19680024852898L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }

    @Test
    public void testComplexRead5() throws Exception {
        run(db, new ComplexRead5(67406588910829L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }

    @Test
    public void testComplexRead6() throws Exception {
        run(db, new ComplexRead6(51078788755688L, 1000L, 2000L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }

    @Test
    public void testComplexRead7() throws Exception {
        run(db, new ComplexRead7(19469276863548L, 1000L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }

    @Test
    public void testComplexRead8() throws Exception {
        run(db, new ComplexRead8(78332612970031L, 0.8f, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }

    @Test
    public void testComplexRead9() throws Exception {
        run(db, new ComplexRead9(44670770561919L, 0.2f, 0.8f, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }

    @Test
    public void testComplexRead10() throws Exception {
        run(db, new ComplexRead10(23457164342167L, 60624749497937L));
    }

    @Test
    public void testComplexRead11() throws Exception {
        run(db, new ComplexRead11(85673929253143L, 3));
    }

    @Test
    public void testComplexRead12() throws Exception {
        run(db, new ComplexRead12(19908707033524L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }

    @Test
    public void testComplexRead13() throws Exception {
        run(db, new ComplexRead13(30924040035422L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2), 10, TruncationOrder.DESC));
    }

    @Test
    public void testSimpleRead1() throws Exception {
        run(db, new SimpleRead1(71195197152144L));
    }

    @Test
    public void testSimpleRead2() throws Exception {
        run(db, new SimpleRead2(44240260404892L, 73377118594155L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2)));
    }

    @Test
    public void testSimpleRead3() throws Exception {
        run(db, new SimpleRead3(82663727934233L));
    }

    @Test
    public void testSimpleRead4() throws Exception {
        run(db, new SimpleRead4(42004037637436L, 10000L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2)));
    }

    @Test
    public void testSimpleRead5() throws Exception {
        run(db, new SimpleRead5(16486343708978L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2)));
    }

    @Test
    public void testSimpleRead6() throws Exception {
        run(db, new SimpleRead6(49955856898317L));
    }

    @Test
    public void testSimpleRead7() throws Exception {
        run(db, new SimpleRead7(43300558283249L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2)));
    }

    @Test
    public void testSimpleRead8() throws Exception {
        run(db, new SimpleRead8(14392749787777L, 10000L, new Date(2023, Calendar.JANUARY, 1),
                new Date(2023, Calendar.JANUARY, 2)));
    }

    @Test
    public void testWrite1() throws Exception {
        run(db, new Write1(46661186336351L, "Alice", 33636683819864L,
                new Date(2023, Calendar.FEBRUARY, 1), false, "Credit"));
    }

    @Test
    public void testWrite2() throws Exception {
        run(db, new Write2(10988200445031L, "LDBC", 46661186336351L,
                new Date(2023, Calendar.FEBRUARY, 1), false, "Credit"));
    }

    @Test
    public void testWrite3() throws Exception {
        run(db, new Write3(10988200445031L, 46661186336351L, new Date(2023, Calendar.FEBRUARY, 1), 1000L));
    }

    @Test
    public void testWrite4() throws Exception {
        run(db, new Write4(71195197152144L, 10988200445031L, new Date(2023, Calendar.FEBRUARY, 1), 1000L));
    }

    @Test
    public void testWrite5() throws Exception {
        run(db, new Write5(71195197152144L, new Date(2023, Calendar.FEBRUARY, 1), 10988200445031L, 1000L));
    }

    @Test
    public void testWrite6() throws Exception {
        run(db, new Write6(10988200445031L, new Date(2023, Calendar.FEBRUARY, 1), 10988200445031L, 1000L));
    }

    @Test
    public void testWrite7() throws Exception {
        run(db, new Write7(71195197152144L, 10988200445031L, false, new Date(2023, Calendar.FEBRUARY, 1)));
    }

    @Test
    public void testWrite8() throws Exception {
        run(db, new Write8(71195197152144L, 10988200445031L, new Date(2023, Calendar.FEBRUARY, 1), 1000L));
    }

    @Test
    public void testWrite9() throws Exception {
        run(db, new Write9(71195197152144L, 10988200445031L, new Date(2023, Calendar.FEBRUARY, 1), 1000L));
    }

    @Test
    public void testWrite10() throws Exception {
        run(db, new Write10(10988200445031L));
    }

    @Test
    public void testWrite11() throws Exception {
        run(db, new Write11(71195197152144L));
    }

    @Test
    public void testWrite12() throws Exception {
        run(db, new Write12(10988200445031L));
    }

    @Test
    public void testWrite13() throws Exception {
        run(db, new Write13(71195197152144L, 10988200445031L, new Date(2023, Calendar.FEBRUARY, 1)));
    }

    @Test
    public void testWrite14() throws Exception {
        run(db, new Write14(71195197152144L));
    }

    @Test
    public void testWrite15() throws Exception {
        run(db, new Write15(10988200445031L));
    }

    @Test
    public void testReadWrite1() throws Exception {
        run(db, new ReadWrite1(10988200445031L, 71195197152144L,
                new Date(2023, Calendar.FEBRUARY, 1), new Date(2023, Calendar.FEBRUARY, 2)));
    }

    @Test
    public void testReadWrite2() throws Exception {
        run(db, new ReadWrite2(10988200445031L, 71195197152144L, 10000L,
                new Date(2023, Calendar.FEBRUARY, 1), new Date(2023, Calendar.FEBRUARY, 2)));
    }

    @Test
    public void testReadWrite3() throws Exception {
        run(db, new ReadWrite3(10988200445031L, 71195197152144L, 10000L,
                new Date(2023, Calendar.FEBRUARY, 1), new Date(2023, Calendar.FEBRUARY, 2)));
    }
}


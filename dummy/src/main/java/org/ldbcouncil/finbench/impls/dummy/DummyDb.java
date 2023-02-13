package org.ldbcouncil.finbench.impls.dummy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.*;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.workloads.transaction.*;

import java.io.IOException;
import java.util.Map;

public class DummyDb extends Db {
    static Logger logger = LogManager.getLogger("DummyDb");

    @Override
    protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {
        logger.info("DummyDb initialized");

        // complex reads
        registerOperationHandler(ComplexRead1.class, ComplexRead1Handler.class);
        registerOperationHandler(ComplexRead2.class, ComplexRead2Handler.class);
        registerOperationHandler(ComplexRead3.class, ComplexRead3Handler.class);
        registerOperationHandler(ComplexRead4.class, ComplexRead4Handler.class);
        registerOperationHandler(ComplexRead5.class, ComplexRead5Handler.class);
        registerOperationHandler(ComplexRead6.class, ComplexRead6Handler.class);
        registerOperationHandler(ComplexRead7.class, ComplexRead7Handler.class);
        registerOperationHandler(ComplexRead8.class, ComplexRead8Handler.class);
        registerOperationHandler(ComplexRead9.class, ComplexRead9Handler.class);
        registerOperationHandler(ComplexRead10.class, ComplexRead10Handler.class);
        registerOperationHandler(ComplexRead11.class, ComplexRead11Handler.class);
        registerOperationHandler(ComplexRead12.class, ComplexRead12Handler.class);
        registerOperationHandler(ComplexRead13.class, ComplexRead13Handler.class);

        // simple reads
        registerOperationHandler(SimpleRead1.class, SimpleRead1Handler.class);
        registerOperationHandler(SimpleRead2.class, SimpleRead2Handler.class);
        registerOperationHandler(SimpleRead3.class, SimpleRead3Handler.class);
        registerOperationHandler(SimpleRead4.class, SimpleRead4Handler.class);
        registerOperationHandler(SimpleRead5.class, SimpleRead5Handler.class);
        registerOperationHandler(SimpleRead6.class, SimpleRead6Handler.class);
        registerOperationHandler(SimpleRead7.class, SimpleRead7Handler.class);
        registerOperationHandler(SimpleRead8.class, SimpleRead8Handler.class);

        // writes
        registerOperationHandler(Write1.class, Write1Handler.class);
        registerOperationHandler(Write2.class, Write2Handler.class);
        registerOperationHandler(Write3.class, Write3Handler.class);
        registerOperationHandler(Write4.class, Write4Handler.class);
        registerOperationHandler(Write5.class, Write5Handler.class);
        registerOperationHandler(Write6.class, Write6Handler.class);
        registerOperationHandler(Write7.class, Write7Handler.class);
        registerOperationHandler(Write8.class, Write8Handler.class);
        registerOperationHandler(Write9.class, Write9Handler.class);
        registerOperationHandler(Write10.class, Write10Handler.class);
        registerOperationHandler(Write11.class, Write11Handler.class);
        registerOperationHandler(Write12.class, Write12Handler.class);
        registerOperationHandler(Write13.class, Write13Handler.class);
        registerOperationHandler(Write14.class, Write14Handler.class);
        registerOperationHandler(Write15.class, Write15Handler.class);

        // read-writes
        registerOperationHandler(ReadWrite1.class, ReadWrite1Handler.class);
        registerOperationHandler(ReadWrite2.class, ReadWrite2Handler.class);
        registerOperationHandler(ReadWrite3.class, ReadWrite3Handler.class);
    }

    @Override
    protected void onClose() throws IOException {
        logger.info("DummyDb closed");
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return null;
    }

    public static class ComplexRead1Handler implements OperationHandler<ComplexRead1, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead1 cr1, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr1.toString());
        }
    }

    public static class ComplexRead2Handler implements OperationHandler<ComplexRead2, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead2 cr2, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr2.toString());
        }
    }

    public static class ComplexRead3Handler implements OperationHandler<ComplexRead3, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead3 cr3, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr3.toString());
        }
    }

    public static class ComplexRead4Handler implements OperationHandler<ComplexRead4, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead4 cr4, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr4.toString());
        }
    }

    public static class ComplexRead5Handler implements OperationHandler<ComplexRead5, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead5 cr5, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr5.toString());
        }
    }

    public static class ComplexRead6Handler implements OperationHandler<ComplexRead6, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead6 cr6, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr6.toString());
        }
    }

    public static class ComplexRead7Handler implements OperationHandler<ComplexRead7, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead7 cr7, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr7.toString());
        }
    }

    public static class ComplexRead8Handler implements OperationHandler<ComplexRead8, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead8 cr8, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr8.toString());
        }
    }

    public static class ComplexRead9Handler implements OperationHandler<ComplexRead9, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead9 cr9, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr9.toString());
        }
    }

    public static class ComplexRead10Handler implements OperationHandler<ComplexRead10, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead10 cr10, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr10.toString());
        }
    }

    public static class ComplexRead11Handler implements OperationHandler<ComplexRead11, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead11 cr11, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr11.toString());
        }
    }

    public static class ComplexRead12Handler implements OperationHandler<ComplexRead12, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead12 cr12, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr12.toString());
        }
    }

    public static class ComplexRead13Handler implements OperationHandler<ComplexRead13, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead13 cr13, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr13.toString());
        }
    }

    public static class SimpleRead1Handler implements OperationHandler<SimpleRead1, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead1 sr1, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr1.toString());
        }
    }

    public static class SimpleRead2Handler implements OperationHandler<SimpleRead2, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead2 sr2, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr2.toString());
        }
    }

    public static class SimpleRead3Handler implements OperationHandler<SimpleRead3, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead3 sr3, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr3.toString());
        }
    }

    public static class SimpleRead4Handler implements OperationHandler<SimpleRead4, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead4 sr4, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr4.toString());
        }
    }

    public static class SimpleRead5Handler implements OperationHandler<SimpleRead5, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead5 sr5, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr5.toString());
        }
    }

    public static class SimpleRead6Handler implements OperationHandler<SimpleRead6, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead6 sr6, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr6.toString());
        }
    }

    public static class SimpleRead7Handler implements OperationHandler<SimpleRead7, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead7 sr7, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr7.toString());
        }
    }

    public static class SimpleRead8Handler implements OperationHandler<SimpleRead8, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead8 sr8, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr8.toString());
        }
    }

    public static class Write1Handler implements OperationHandler<Write1, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write1 w1, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w1.toString());
        }
    }

    public static class Write2Handler implements OperationHandler<Write2, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write2 w2, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w2.toString());
        }
    }

    public static class Write3Handler implements OperationHandler<Write3, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write3 w3, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w3.toString());
        }
    }

    public static class Write4Handler implements OperationHandler<Write4, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write4 w4, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w4.toString());
        }
    }

    public static class Write5Handler implements OperationHandler<Write5, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write5 w5, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w5.toString());
        }
    }

    public static class Write6Handler implements OperationHandler<Write6, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write6 w6, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w6.toString());
        }
    }

    public static class Write7Handler implements OperationHandler<Write7, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write7 w7, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w7.toString());
        }
    }

    public static class Write8Handler implements OperationHandler<Write8, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write8 w8, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w8.toString());
        }
    }

    public static class Write9Handler implements OperationHandler<Write9, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write9 w9, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w9.toString());
        }
    }

    public static class Write10Handler implements OperationHandler<Write10, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write10 w10, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w10.toString());
        }
    }

    public static class Write11Handler implements OperationHandler<Write11, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write11 w11, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w11.toString());
        }
    }

    public static class Write12Handler implements OperationHandler<Write12, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write12 w12, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w12.toString());
        }
    }

    public static class Write13Handler implements OperationHandler<Write13, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write13 w13, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w13.toString());
        }
    }

    public static class Write14Handler implements OperationHandler<Write14, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write14 w14, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w14.toString());
        }
    }

    public static class Write15Handler implements OperationHandler<Write15, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write15 w15, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w15.toString());
        }
    }

    public static class ReadWrite1Handler implements OperationHandler<ReadWrite1, DummyDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite1 rw1, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(rw1.toString());
        }
    }

    public static class ReadWrite2Handler implements OperationHandler<ReadWrite2, DummyDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite2 rw2, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(rw2.toString());
        }
    }

    public static class ReadWrite3Handler implements OperationHandler<ReadWrite3, DummyDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite3 rw3, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(rw3.toString());
        }
    }

}

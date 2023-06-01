package org.ldbcouncil.finbench.impls.tugraph;

import com.antgroup.tugraph.TuGraphDbRpcClient;
import org.apache.commons.codec.binary.Base64;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.*;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;

import java.io.IOException;
import java.util.Map;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;


public class TuGraphTransactionDb extends Db {
    static Logger logger = LogManager.getLogger("TuGraph");

    TuGraphDbConnectionState dcs;

    // Return byte type of 1 if it's ordered DESC
    private static byte encodeTruncationOrder(TruncationOrder truncationOrder) {
        return (byte) (truncationOrder == TruncationOrder.TIMESTAMP_DESCENDING ? 1 : 0);
    }

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        logger.info("TuGraphTransactionDb initialized");

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

        // simple reads
        registerOperationHandler(SimpleRead1.class, SimpleRead1Handler.class);
        registerOperationHandler(SimpleRead2.class, SimpleRead2Handler.class);
        registerOperationHandler(SimpleRead3.class, SimpleRead3Handler.class);
        registerOperationHandler(SimpleRead4.class, SimpleRead4Handler.class);
        registerOperationHandler(SimpleRead5.class, SimpleRead5Handler.class);
        registerOperationHandler(SimpleRead6.class, SimpleRead6Handler.class);

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

        // read-writes
        registerOperationHandler(ReadWrite1.class, ReadWrite1Handler.class);
        registerOperationHandler(ReadWrite2.class, ReadWrite2Handler.class);
        registerOperationHandler(ReadWrite3.class, ReadWrite3Handler.class);
    }

    @Override
    protected void onClose() throws IOException {
        logger.info("TuGraphTransactionDb closed");
        dcs.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return dcs;
    }

    public static class ComplexRead1Handler implements OperationHandler<ComplexRead1, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead1 cr1, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {

            try {
                CustomDataOutputStream cdos = new CustomDataOutputStream();
                cdos.writeInt64(cr1.getId());
                cdos.writeInt64(cr1.getStartTime().getTime());
                cdos.writeInt64(cr1.getEndTime().getTime());
                cdos.writeInt64(cr1.getTruncationLimit());
                cdos.writeByte(encodeTruncationOrder(cr1.getTruncationOrder()));
                // TODO: call procedure
                String input = Base64.encodeBase64String(cdos.toByteArray());
                TuGraphDbRpcClient client = dbConnectionState.popClient();
                dbConnectionState.pushClient(client);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static class ComplexRead2Handler implements OperationHandler<ComplexRead2, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead2 cr2, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ComplexRead3Handler implements OperationHandler<ComplexRead3, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead3 cr3, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ComplexRead4Handler implements OperationHandler<ComplexRead4, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead4 cr4, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ComplexRead5Handler implements OperationHandler<ComplexRead5, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead5 cr5, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ComplexRead6Handler implements OperationHandler<ComplexRead6, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead6 cr6, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ComplexRead7Handler implements OperationHandler<ComplexRead7, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead7 cr7, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ComplexRead8Handler implements OperationHandler<ComplexRead8, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead8 cr8, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ComplexRead9Handler implements OperationHandler<ComplexRead9, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead9 cr9, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ComplexRead10Handler implements OperationHandler<ComplexRead10, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead10 cr10, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ComplexRead11Handler implements OperationHandler<ComplexRead11, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead11 cr11, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ComplexRead12Handler implements OperationHandler<ComplexRead12, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead12 cr12, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class SimpleRead1Handler implements OperationHandler<SimpleRead1, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead1 sr1, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class SimpleRead2Handler implements OperationHandler<SimpleRead2, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead2 sr2, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class SimpleRead3Handler implements OperationHandler<SimpleRead3, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead3 sr3, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class SimpleRead4Handler implements OperationHandler<SimpleRead4, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead4 sr4, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class SimpleRead5Handler implements OperationHandler<SimpleRead5, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead5 sr5, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class SimpleRead6Handler implements OperationHandler<SimpleRead6, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead6 sr6, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write1Handler implements OperationHandler<Write1, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write1 w1, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write2Handler implements OperationHandler<Write2, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write2 w2, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write3Handler implements OperationHandler<Write3, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write3 w3, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write4Handler implements OperationHandler<Write4, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write4 w4, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write5Handler implements OperationHandler<Write5, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write5 w5, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write6Handler implements OperationHandler<Write6, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write6 w6, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write7Handler implements OperationHandler<Write7, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write7 w7, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write8Handler implements OperationHandler<Write8, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write8 w8, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write9Handler implements OperationHandler<Write9, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write9 w9, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write10Handler implements OperationHandler<Write10, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write10 w10, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write11Handler implements OperationHandler<Write11, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write11 w11, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write12Handler implements OperationHandler<Write12, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write12 w12, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class Write13Handler implements OperationHandler<Write13, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(Write13 w13, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ReadWrite1Handler implements OperationHandler<ReadWrite1, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite1 rw1, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ReadWrite2Handler implements OperationHandler<ReadWrite2, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite2 rw2, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }

    public static class ReadWrite3Handler implements OperationHandler<ReadWrite3, TuGraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite3 rw3, TuGraphDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            // TODO: do as above
        }
    }
}

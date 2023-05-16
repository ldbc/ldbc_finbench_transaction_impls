package org.ldbcouncil.finbench.impls.galaxybase;

import com.graphdbapi.driver.v1.Record;
import org.ldbcouncil.finbench.impls.galaxybase.operationhandlers.GalaxybaseListOperationHandler;
import org.ldbcouncil.finbench.impls.galaxybase.operationhandlers.GalaxybaseTransactionUpdateOperationHandler;
import org.ldbcouncil.finbench.impls.galaxybase.operationhandlers.GalaxybaseUpdateOperationHandler;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.DbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.result.Path;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write13;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write9;

public class GalaxybaseDb extends Db {
    static Logger logger = LogManager.getLogger("GalaxybaseDb");
    GalaxybaseDbConnectionState dcs;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        logger.info("Galaxybase initialized");
        dcs = new GalaxybaseDbConnectionState(properties, new GalaxybaseQueryStore(properties.get("queryDir")));

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
        logger.info("Galaxybase closed");
        dcs.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return dcs;
    }

    public static class ComplexRead1Handler extends
        GalaxybaseListOperationHandler<ComplexRead1Result, ComplexRead1> {

        @Override
        protected ComplexRead1Result convertSingleResult(Record record) {
            ComplexRead1Result result = new ComplexRead1Result(
                Long.parseLong(record.get(0).asString()),
                record.get(1).asInt(),
                Long.parseLong(record.get(2).asString()),
                record.get(3).asString());
            return result;
        }

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead1 operation) {
            return state.getQueryStore().getComplexRead1(operation);
        }
    }

    public static class ComplexRead2Handler extends
        GalaxybaseListOperationHandler<ComplexRead2Result, ComplexRead2> {

        @Override
        protected ComplexRead2Result convertSingleResult(Record record) {
            ComplexRead2Result result = new ComplexRead2Result(
                Long.parseLong(record.get(0).asString()),
                record.get(1).asDouble(),
                record.get(2).asDouble());
            return result;
        }

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead2 operation) {
            return state.getQueryStore().getComplexRead2(operation);
        }
    }

    public static class ComplexRead3Handler extends
        GalaxybaseListOperationHandler<ComplexRead3Result, ComplexRead3> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead3 operation) {
            return state.getQueryStore().getComplexRead3(operation);
        }


        @Override
        protected ComplexRead3Result convertSingleResult(Record record) {
            ComplexRead3Result result = new ComplexRead3Result(record.get(0).asLong());
            return result;
        }
    }

    public static class ComplexRead4Handler extends
        GalaxybaseListOperationHandler<ComplexRead4Result, ComplexRead4> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead4 operation) {
            return state.getQueryStore().getComplexRead4(operation);
        }


        @Override
        protected ComplexRead4Result convertSingleResult(Record record) {
            ComplexRead4Result result = new ComplexRead4Result(
                Long.parseLong(record.get(0).asString()),
                record.get(1).asLong(),
                record.get(2).asDouble(),
                record.get(3).asDouble(),
                record.get(4).asLong(),
                record.get(5).asDouble(),
                record.get(6).asDouble());
            return result;
        }
    }

    public static class ComplexRead5Handler extends
        GalaxybaseListOperationHandler<ComplexRead5Result, ComplexRead5> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead5 operation) {
            return state.getQueryStore().getComplexRead5(operation);
        }


        @Override
        protected ComplexRead5Result convertSingleResult(Record record) {
            Path path = new Path();
            List<Object> ids = record.get(0).asList();
            for (Object id : ids) {
                path.addId(Long.parseLong((String) id));
            }
            ComplexRead5Result result = new ComplexRead5Result(path);
            return result;
        }
    }

    public static class ComplexRead6Handler extends
        GalaxybaseListOperationHandler<ComplexRead6Result, ComplexRead6> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead6 operation) {
            return state.getQueryStore().getComplexRead6(operation);
        }


        @Override
        protected ComplexRead6Result convertSingleResult(Record record) {
            ComplexRead6Result result = new ComplexRead6Result(
                Long.parseLong(record.get(0).asString()),
                record.get(1).asDouble(),
                record.get(2).asDouble());
            return result;
        }
    }

    public static class ComplexRead7Handler extends
        GalaxybaseListOperationHandler<ComplexRead7Result, ComplexRead7> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead7 operation) {
            return state.getQueryStore().getComplexRead7(operation);
        }

        @Override
        protected ComplexRead7Result convertSingleResult(Record record) {
            ComplexRead7Result result = new ComplexRead7Result(
                record.get(0).asInt(),
                record.get(1).asInt(),
                (float) record.get(2).asDouble());
            return result;
        }
    }

    public static class ComplexRead8Handler extends
        GalaxybaseListOperationHandler<ComplexRead8Result, ComplexRead8> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead8 operation) {
            return state.getQueryStore().getComplexRead8(operation);
        }


        @Override
        protected ComplexRead8Result convertSingleResult(Record record) {
            ComplexRead8Result result = new ComplexRead8Result(
                Long.parseLong(record.get(0).asString()),
                (float) record.get(1).asDouble(),
                record.get(2).asInt());
            return result;
        }
    }

    public static class ComplexRead9Handler extends
        GalaxybaseListOperationHandler<ComplexRead9Result, ComplexRead9> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead9 operation) {
            return state.getQueryStore().getComplexRead9(operation);
        }


        @Override
        protected ComplexRead9Result convertSingleResult(Record record) {
            ComplexRead9Result result = new ComplexRead9Result(
                (float) record.get(0).asDouble(),
                (float) record.get(1).asDouble(),
                (float) record.get(2).asDouble());
            return result;
        }
    }

    public static class ComplexRead10Handler extends
        GalaxybaseListOperationHandler<ComplexRead10Result, ComplexRead10> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead10 operation) {
            return state.getQueryStore().getComplexRead10(operation);
        }


        @Override
        protected ComplexRead10Result convertSingleResult(Record record) {
            ComplexRead10Result result = new ComplexRead10Result(
                (float) record.get(0).asDouble());
            return result;
        }
    }

    public static class ComplexRead11Handler extends
        GalaxybaseListOperationHandler<ComplexRead11Result, ComplexRead11> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead11 operation) {
            return state.getQueryStore().getComplexRead11(operation);
        }


        @Override
        protected ComplexRead11Result convertSingleResult(Record record) {
            ComplexRead11Result result = new ComplexRead11Result(
                record.get(0).asDouble(),
                record.get(1).asInt());
            return result;
        }
    }

    public static class ComplexRead12Handler extends
        GalaxybaseListOperationHandler<ComplexRead12Result, ComplexRead12> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ComplexRead12 operation) {
            return state.getQueryStore().getComplexRead12(operation);
        }


        @Override
        protected ComplexRead12Result convertSingleResult(Record record) {
            ComplexRead12Result result = new ComplexRead12Result(
                Long.parseLong(record.get(0).asString()),
                record.get(1).asDouble());
            return result;
        }
    }

    public static class SimpleRead1Handler extends
        GalaxybaseListOperationHandler<SimpleRead1Result, SimpleRead1> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, SimpleRead1 operation) {
            return state.getQueryStore().getSimpleRead1(operation);
        }


        @Override
        protected SimpleRead1Result convertSingleResult(Record record) {
            SimpleRead1Result result = new SimpleRead1Result(
                new Date(record.get(0).asLong()),
                record.get(1).asBoolean(),
                record.get(2).asString());
            return result;
        }
    }

    public static class SimpleRead2Handler extends
        GalaxybaseListOperationHandler<SimpleRead2Result, SimpleRead2> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, SimpleRead2 operation) {
            return state.getQueryStore().getSimpleRead2(operation);
        }

        @Override
        protected SimpleRead2Result convertSingleResult(Record record) {
            SimpleRead2Result result = new SimpleRead2Result(
                record.get(0).asDouble(),
                record.get(1).asDouble(),
                record.get(2).asLong(),
                record.get(3).asDouble(),
                record.get(4).asDouble(),
                record.get(5).asLong());
            return result;
        }
    }

    public static class SimpleRead3Handler extends
        GalaxybaseListOperationHandler<SimpleRead3Result, SimpleRead3> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, SimpleRead3 operation) {
            return state.getQueryStore().getSimpleRead3(operation);
        }

        @Override
        protected SimpleRead3Result convertSingleResult(Record record) {
            SimpleRead3Result result = new SimpleRead3Result(
                (float) record.get(0).asDouble());
            return result;
        }
    }

    public static class SimpleRead4Handler extends
        GalaxybaseListOperationHandler<SimpleRead4Result, SimpleRead4> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, SimpleRead4 operation) {
            return state.getQueryStore().getSimpleRead4(operation);
        }

        @Override
        protected SimpleRead4Result convertSingleResult(Record record) {
            SimpleRead4Result result = new SimpleRead4Result(
                Long.parseLong(record.get(0).asString()),
                record.get(1).asInt(),
                record.get(2).asDouble());
            return result;
        }
    }

    public static class SimpleRead5Handler extends
        GalaxybaseListOperationHandler<SimpleRead5Result, SimpleRead5> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, SimpleRead5 operation) {
            return state.getQueryStore().getSimpleRead5(operation);
        }

        @Override
        protected SimpleRead5Result convertSingleResult(Record record) {
            SimpleRead5Result result = new SimpleRead5Result(
                Long.parseLong(record.get(0).asString()),
                record.get(1).asInt(),
                record.get(2).asDouble());
            return result;
        }
    }

    public static class SimpleRead6Handler extends
        GalaxybaseListOperationHandler<SimpleRead6Result, SimpleRead6> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, SimpleRead6 operation) {
            return state.getQueryStore().getSimpleRead6(operation);
        }

        @Override
        protected SimpleRead6Result convertSingleResult(Record record) {
            SimpleRead6Result result = new SimpleRead6Result(
                Long.parseLong(record.get(0).asString()));
            return result;
        }
    }

    public static class Write1Handler extends
        GalaxybaseUpdateOperationHandler<Write1> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write1 operation) {
            return state.getQueryStore().getWrite1(operation);
        }
    }

    public static class Write2Handler extends
        GalaxybaseUpdateOperationHandler<Write2> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write2 operation) {
            return state.getQueryStore().getWrite2(operation);
        }
    }

    public static class Write3Handler extends
        GalaxybaseUpdateOperationHandler<Write3> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write3 operation) {
            return state.getQueryStore().getWrite3(operation);
        }
    }

    public static class Write4Handler extends
        GalaxybaseUpdateOperationHandler<Write4> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write4 operation) {
            return state.getQueryStore().getWrite4(operation);
        }
    }

    public static class Write5Handler extends
        GalaxybaseUpdateOperationHandler<Write5> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write5 operation) {
            return state.getQueryStore().getWrite5(operation);
        }
    }

    public static class Write6Handler extends
        GalaxybaseUpdateOperationHandler<Write6> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write6 operation) {
            return state.getQueryStore().getWrite6(operation);
        }
    }

    public static class Write7Handler extends
        GalaxybaseUpdateOperationHandler<Write7> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write7 operation) {
            return state.getQueryStore().getWrite7(operation);
        }
    }

    public static class Write8Handler extends
        GalaxybaseUpdateOperationHandler<Write8> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write8 operation) {
            return state.getQueryStore().getWrite8(operation);
        }
    }

    public static class Write9Handler extends
        GalaxybaseUpdateOperationHandler<Write9> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write9 operation) {
            return state.getQueryStore().getWrite9(operation);
        }
    }

    public static class Write10Handler extends
        GalaxybaseUpdateOperationHandler<Write10> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write10 operation) {
            return state.getQueryStore().getWrite10(operation);
        }
    }

    public static class Write11Handler extends
        GalaxybaseUpdateOperationHandler<Write11> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write11 operation) {
            return state.getQueryStore().getWrite11(operation);
        }
    }

    public static class Write12Handler extends
        GalaxybaseUpdateOperationHandler<Write12> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write12 operation) {
            return state.getQueryStore().getWrite12(operation);
        }
    }

    public static class Write13Handler extends
        GalaxybaseUpdateOperationHandler<Write13> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, Write13 operation) {
            return state.getQueryStore().getWrite13(operation);
        }
    }

    public static class ReadWrite1Handler extends
        GalaxybaseTransactionUpdateOperationHandler<ReadWrite1> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ReadWrite1 operation) {
            return state.getQueryStore().getReadWrite1(operation);
        }
    }

    public static class ReadWrite2Handler extends
        GalaxybaseTransactionUpdateOperationHandler<ReadWrite2> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ReadWrite2 operation) {
            return state.getQueryStore().getReadWrite2(operation);
        }
    }

    public static class ReadWrite3Handler extends
        GalaxybaseTransactionUpdateOperationHandler<ReadWrite3> {

        @Override
        public String getQueryString(GalaxybaseDbConnectionState state, ReadWrite3 operation) {
            return state.getQueryStore().getReadWrite3(operation);
        }
    }
}

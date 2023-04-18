package org.ldbcouncil.finbench.impls.galaxybase;

import java.io.IOException;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.DbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;
import org.ldbcouncil.finbench.impls.galaxybase.converter.GalaxybaseConverter;
import org.ldbcouncil.finbench.impls.galaxybase.operationhandlers.GalaxybaseListOperationHandler;
import org.ldbcouncil.finbench.impls.galaxybase.operationhandlers.GalaxybaseUpdateOperationHandler;

public class GalaxybaseDb extends Db {
    static Logger logger = LogManager.getLogger("GalaxybaseDb");
    GalaxybaseDbConnectionState dcs;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        logger.info("Galaxybase initialized");
        dcs = new GalaxybaseDbConnectionState(properties);

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
        protected String getQueryName() {
            return "complexRead1";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead1 operation) {
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getTruncationLimit(),
                operation.getTruncationOrder());
        }

        @Override
        protected ComplexRead1Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            ComplexRead1Result result = new ComplexRead1Result(
                Long.parseLong(results[0]),
                Integer.parseInt(results[1]),
                Long.parseLong(results[2]),
                results[3]);
            return result;
        }
    }

    public static class ComplexRead2Handler extends
        GalaxybaseListOperationHandler<ComplexRead2Result, ComplexRead2> {

        @Override
        protected String getQueryName() {
            return "ComplexRead2";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead2 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead2Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead2Result result = new ComplexRead2Result();
            return null;
        }
    }

    public static class ComplexRead3Handler extends
        GalaxybaseListOperationHandler<ComplexRead3Result, ComplexRead3> {

        @Override
        protected String getQueryName() {
            return "ComplexRead3";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead3 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead3Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead3Result result = new ComplexRead3Result();
            return null;
        }
    }

    public static class ComplexRead4Handler extends
        GalaxybaseListOperationHandler<ComplexRead4Result, ComplexRead4> {

        @Override
        protected String getQueryName() {
            return "ComplexRead4";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead4 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead4Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead4Result result = new ComplexRead4Result();
            return null;
        }
    }

    public static class ComplexRead5Handler extends
        GalaxybaseListOperationHandler<ComplexRead5Result, ComplexRead5> {

        @Override
        protected String getQueryName() {
            return "ComplexRead5";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead5 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead5Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead5Result result = new ComplexRead5Result();
            return null;
        }
    }

    public static class ComplexRead6Handler extends
        GalaxybaseListOperationHandler<ComplexRead6Result, ComplexRead6> {

        @Override
        protected String getQueryName() {
            return "ComplexRead6";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead6 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead6Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead6Result result = new ComplexRead6Result();
            return null;
        }
    }

    public static class ComplexRead7Handler extends
        GalaxybaseListOperationHandler<ComplexRead7Result, ComplexRead7> {

        @Override
        protected String getQueryName() {
            return "ComplexRead7";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead7 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead7Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead7Result result = new ComplexRead7Result();
            return null;
        }
    }

    public static class ComplexRead8Handler extends
        GalaxybaseListOperationHandler<ComplexRead8Result, ComplexRead8> {

        @Override
        protected String getQueryName() {
            return "ComplexRead8";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead8 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead8Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead8Result result = new ComplexRead8Result();
            return null;
        }
    }

    public static class ComplexRead9Handler extends
        GalaxybaseListOperationHandler<ComplexRead9Result, ComplexRead9> {

        @Override
        protected String getQueryName() {
            return "ComplexRead9";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead9 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead9Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead9Result result = new ComplexRead9Result();
            return null;
        }
    }

    public static class ComplexRead10Handler extends
        GalaxybaseListOperationHandler<ComplexRead10Result, ComplexRead10> {

        @Override
        protected String getQueryName() {
            return "ComplexRead10";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead10 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead10Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead10Result result = new ComplexRead10Result();
            return null;
        }
    }

    public static class ComplexRead11Handler extends
        GalaxybaseListOperationHandler<ComplexRead11Result, ComplexRead11> {

        @Override
        protected String getQueryName() {
            return "ComplexRead11";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead11 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead11Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead11Result result = new ComplexRead11Result();
            return null;
        }
    }

    public static class ComplexRead12Handler extends
        GalaxybaseListOperationHandler<ComplexRead12Result, ComplexRead12> {

        @Override
        protected String getQueryName() {
            return "ComplexRead12";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead12 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead12Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead12Result result = new ComplexRead12Result();
            return null;
        }
    }

    public static class ComplexRead13Handler extends
        GalaxybaseListOperationHandler<ComplexRead13Result, ComplexRead13> {

        @Override
        protected String getQueryName() {
            return "ComplexRead13";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ComplexRead13 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected ComplexRead13Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // ComplexRead13Result result = new ComplexRead13Result();
            return null;
        }
    }

    public static class SimpleRead1Handler extends
        GalaxybaseListOperationHandler<SimpleRead1Result, SimpleRead1> {

        @Override
        protected String getQueryName() {
            return "SimpleRead1";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, SimpleRead1 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected SimpleRead1Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // SimpleRead1Result result = new SimpleRead1Result();
            return null;
        }
    }

    public static class SimpleRead2Handler extends
        GalaxybaseListOperationHandler<SimpleRead2Result, SimpleRead2> {

        @Override
        protected String getQueryName() {
            return "SimpleRead2";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, SimpleRead2 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected SimpleRead2Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // SimpleRead2Result result = new SimpleRead2Result();
            return null;
        }
    }

    public static class SimpleRead3Handler extends
        GalaxybaseListOperationHandler<SimpleRead3Result, SimpleRead3> {

        @Override
        protected String getQueryName() {
            return "SimpleRead3";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, SimpleRead3 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected SimpleRead3Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // SimpleRead3Result result = new SimpleRead3Result();
            return null;
        }
    }

    public static class SimpleRead4Handler extends
        GalaxybaseListOperationHandler<SimpleRead4Result, SimpleRead4> {

        @Override
        protected String getQueryName() {
            return "SimpleRead4";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, SimpleRead4 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected SimpleRead4Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // SimpleRead4Result result = new SimpleRead4Result();
            return null;
        }
    }

    public static class SimpleRead5Handler extends
        GalaxybaseListOperationHandler<SimpleRead5Result, SimpleRead5> {

        @Override
        protected String getQueryName() {
            return "SimpleRead5";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, SimpleRead5 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected SimpleRead5Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // SimpleRead5Result result = new SimpleRead5Result();
            return null;
        }
    }

    public static class SimpleRead6Handler extends
        GalaxybaseListOperationHandler<SimpleRead6Result, SimpleRead6> {

        @Override
        protected String getQueryName() {
            return "SimpleRead6";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, SimpleRead6 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected SimpleRead6Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // SimpleRead6Result result = new SimpleRead6Result();
            return null;
        }
    }

    public static class SimpleRead7Handler extends
        GalaxybaseListOperationHandler<SimpleRead7Result, SimpleRead7> {

        @Override
        protected String getQueryName() {
            return "SimpleRead7";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, SimpleRead7 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected SimpleRead7Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // SimpleRead7Result result = new SimpleRead7Result();
            return null;
        }
    }

    public static class SimpleRead8Handler extends
        GalaxybaseListOperationHandler<SimpleRead8Result, SimpleRead8> {

        @Override
        protected String getQueryName() {
            return "SimpleRead8";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, SimpleRead8 operation) {
            // TODO add params
            return null;
        }

        @Override
        protected SimpleRead8Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            // TODO deal result
            // SimpleRead8Result result = new SimpleRead8Result();
            return null;
        }
    }

    public static class Write1Handler extends
        GalaxybaseUpdateOperationHandler<Write1> {

        @Override
        protected String getQueryName() {
            return "Write1";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write1 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write2Handler extends
        GalaxybaseUpdateOperationHandler<Write2> {

        @Override
        protected String getQueryName() {
            return "Write2";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write2 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write3Handler extends
        GalaxybaseUpdateOperationHandler<Write3> {

        @Override
        protected String getQueryName() {
            return "Write3";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write3 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write4Handler extends
        GalaxybaseUpdateOperationHandler<Write4> {

        @Override
        protected String getQueryName() {
            return "Write4";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write4 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write5Handler extends
        GalaxybaseUpdateOperationHandler<Write5> {

        @Override
        protected String getQueryName() {
            return "Write5";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write5 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write6Handler extends
        GalaxybaseUpdateOperationHandler<Write6> {

        @Override
        protected String getQueryName() {
            return "Write6";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write6 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write7Handler extends
        GalaxybaseUpdateOperationHandler<Write7> {

        @Override
        protected String getQueryName() {
            return "Write7";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write7 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write8Handler extends
        GalaxybaseUpdateOperationHandler<Write8> {

        @Override
        protected String getQueryName() {
            return "Write8";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write8 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write9Handler extends
        GalaxybaseUpdateOperationHandler<Write9> {

        @Override
        protected String getQueryName() {
            return "Write9";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write9 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write10Handler extends
        GalaxybaseUpdateOperationHandler<Write10> {

        @Override
        protected String getQueryName() {
            return "Write10";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write10 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write11Handler extends
        GalaxybaseUpdateOperationHandler<Write11> {

        @Override
        protected String getQueryName() {
            return "Write11";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write11 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write12Handler extends
        GalaxybaseUpdateOperationHandler<Write12> {

        @Override
        protected String getQueryName() {
            return "Write12";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write12 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write13Handler extends
        GalaxybaseUpdateOperationHandler<Write13> {

        @Override
        protected String getQueryName() {
            return "Write13";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write13 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write14Handler extends
        GalaxybaseUpdateOperationHandler<Write14> {

        @Override
        protected String getQueryName() {
            return "Write14";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write14 operation) {
            // TODO add params
            return null;
        }
    }

    public static class Write15Handler extends
        GalaxybaseUpdateOperationHandler<Write15> {

        @Override
        protected String getQueryName() {
            return "Write15";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, Write15 operation) {
            // TODO add params
            return null;
        }
    }

    public static class ReadWrite1Handler extends
        GalaxybaseUpdateOperationHandler<ReadWrite1> {

        @Override
        protected String getQueryName() {
            return "ReadWrite1";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ReadWrite1 operation) {
            // TODO add params
            return null;
        }
    }

    public static class ReadWrite2Handler extends
        GalaxybaseUpdateOperationHandler<ReadWrite2> {

        @Override
        protected String getQueryName() {
            return "ReadWrite2";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ReadWrite2 operation) {
            // TODO add params
            return null;
        }
    }

    public static class ReadWrite3Handler extends
        GalaxybaseUpdateOperationHandler<ReadWrite3> {

        @Override
        protected String getQueryName() {
            return "ReadWrite3";
        }

        @Override
        protected String getQueryParam(GalaxybaseDbConnectionState state, ReadWrite3 operation) {
            // TODO add params
            return null;
        }
    }
}

/*
package org.ldbcouncil.finbench.impls.galaxybase;

import java.io.IOException;
import java.util.Date;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getTruncationLimit(),
                operation.getTruncationOrder());
        }

        @Override
        protected ComplexRead2Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            ComplexRead2Result result = new ComplexRead2Result(
                Long.parseLong(results[0]),
                Long.parseLong(results[1]),
                Long.parseLong(results[2]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId1(),
                operation.getId2(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getTruncationLimit(),
                operation.getTruncationOrder());
        }

        @Override
        protected ComplexRead3Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            ComplexRead3Result result = new ComplexRead3Result(Long.parseLong(results[0]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId1(),
                operation.getId2(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getTruncationLimit(),
                operation.getTruncationOrder());
        }

        @Override
        protected ComplexRead4Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            ComplexRead4Result result = new ComplexRead4Result(
                Long.parseLong(results[0]),
                Long.parseLong(results[1]),
                Long.parseLong(results[2]),
                Long.parseLong(results[3]),
                Long.parseLong(results[4]),
                Long.parseLong(results[5]),
                Long.parseLong(results[6]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getTruncationLimit(),
                operation.getTruncationOrder());
        }

        @Override
        protected ComplexRead5Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            Path path = new Path();
            String[] ids = results[0].split(";", 10000);
            for (String id : ids) {
                path.addId(Long.parseLong(id));
            }
            ComplexRead5Result result = new ComplexRead5Result(path);
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getThreshold1(),
                operation.getThreshold2(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getTruncationLimit(),
                operation.getTruncationOrder());
        }

        @Override
        protected ComplexRead6Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            ComplexRead6Result result = new ComplexRead6Result(
                Long.parseLong(results[0]),
                Long.parseLong(results[1]),
                Long.parseLong(results[2]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getThreshold(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getTruncationLimit(),
                operation.getTruncationOrder());
        }

        @Override
        protected ComplexRead7Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            ComplexRead7Result result = new ComplexRead7Result(
                Integer.parseInt(results[0]),
                Integer.parseInt(results[1]),
                Float.parseFloat(results[2]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getThreshold(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getTruncationLimit(),
                operation.getTruncationOrder());
        }

        @Override
        protected ComplexRead8Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            ComplexRead8Result result = new ComplexRead8Result(
                Long.parseLong(results[0]),
                Float.parseFloat(results[1]),
                Integer.parseInt(results[2]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getThreshold(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getTruncationLimit(),
                operation.getTruncationOrder());
        }

        @Override
        protected ComplexRead9Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            ComplexRead9Result result = new ComplexRead9Result(
                Float.parseFloat(results[0]),
                Float.parseFloat(results[1]),
                Float.parseFloat(results[2]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getPid1(),
                operation.getPid2(),
                operation.getStartTime(),
                operation.getEndTime());
        }

        @Override
        protected ComplexRead10Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            ComplexRead10Result result = new ComplexRead10Result(
                Float.parseFloat(results[0]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getTruncationLimit(),
                operation.getTruncationOrder());
        }

        @Override
        protected ComplexRead11Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            ComplexRead11Result result = new ComplexRead11Result(
                Long.parseLong(results[0]),
                Integer.parseInt(results[1]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getTruncationLimit(),
                operation.getTruncationOrder());
        }

        @Override
        protected ComplexRead12Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            ComplexRead12Result result = new ComplexRead12Result(
                Long.parseLong(results[0]),
                Long.parseLong(results[1]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId());
        }

        @Override
        protected SimpleRead1Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            SimpleRead1Result result = new SimpleRead1Result(
                new Date(Long.parseLong(results[0])),
                Boolean.parseBoolean(results[1]),
                results[2]);
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getStartTime(),
                operation.getEndTime());
        }

        @Override
        protected SimpleRead2Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            SimpleRead2Result result = new SimpleRead2Result(
                Long.parseLong(results[0]),
                Long.parseLong(results[1]),
                Long.parseLong(results[2]),
                Long.parseLong(results[3]),
                Long.parseLong(results[4]),
                Long.parseLong(results[5]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getThreshold(),
                operation.getStartTime(),
                operation.getEndTime());
        }

        @Override
        protected SimpleRead3Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            SimpleRead3Result result = new SimpleRead3Result(
                Float.parseFloat(results[0]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getThreshold(),
                operation.getStartTime(),
                operation.getEndTime());
        }

        @Override
        protected SimpleRead4Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            SimpleRead4Result result = new SimpleRead4Result(
                Long.parseLong(results[0]),
                Integer.parseInt(results[1]),
                Long.parseLong(results[2]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getThreshold(),
                operation.getStartTime(),
                operation.getEndTime());
        }

        @Override
        protected SimpleRead5Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            SimpleRead5Result result = new SimpleRead5Result(
                Long.parseLong(results[0]),
                Integer.parseInt(results[1]),
                Long.parseLong(results[2]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getId(),
                operation.getStartTime(),
                operation.getEndTime());
        }

        @Override
        protected SimpleRead6Result convertSingleResult(GalaxybaseDbConnectionState state, String[] results) {
            SimpleRead6Result result = new SimpleRead6Result(
                Long.parseLong(results[0]));
            return result;
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
            return GalaxybaseConverter.getParams(
                operation.getPersonId(),
                operation.getPersonName(),
                operation.getAccountId(),
                operation.timeStamp(),
                operation.getAccountBlocked(),
                operation.getAccountType());
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
            return GalaxybaseConverter.getParams(
                operation.getCompanyId(),
                operation.getCompanyName(),
                operation.getAccountId(),
                operation.getTime(),
                operation.getAccountBlocked(),
                operation.getAccountType());
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
            return GalaxybaseConverter.getParams(
                operation.getSrcId(),
                operation.getDstId(),
                operation.getTime(),
                operation.getAmount());
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
            return GalaxybaseConverter.getParams(
                operation.getSrcId(),
                operation.getDstId(),
                operation.getTime(),
                operation.getAmount());
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
            return GalaxybaseConverter.getParams(
                operation.getPersonId(),
                operation.getTime(),
                operation.getLoanId(),
                operation.getAmount());
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
            return GalaxybaseConverter.getParams(
                operation.getCompanyId(),
                operation.getTime(),
                operation.getLoanId(),
                operation.getTime());
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
            return GalaxybaseConverter.getParams(
                operation.getAccountId(),
                operation.getMediumId(),
                operation.getMediumBlocked(),
                operation.getTime());
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
            return GalaxybaseConverter.getParams(
                operation.getAccountId(),
                operation.getLoanId(),
                operation.getTime(),
                operation.getAmount());
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
            return GalaxybaseConverter.getParams(
                operation.getAccountId(),
                operation.getLoanId(),
                operation.getTime(),
                operation.getAmount());
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
            return GalaxybaseConverter.getParams(
                operation.getAccountId());
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
            return GalaxybaseConverter.getParams(
                operation.getPersonId());
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
            return GalaxybaseConverter.getParams(
                operation.getPid1(),
                operation.getPid2(),
                operation.getTime());
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
            return GalaxybaseConverter.getParams(
                operation.getId());
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
            return GalaxybaseConverter.getParams(
                operation.getSrcId(),
                operation.getDstId(),
                operation.getTime(),
                operation.getAmount(),
                operation.getStartTime(),
                operation.getEndTime());
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
            return GalaxybaseConverter.getParams(
                operation.getSrcId(),
                operation.getDstId(),
                operation.getTime(),
                operation.getAmount(),
                operation.getAmountThreshold(),
                operation.getStartTime(),
                operation.getEndTime(),
                operation.getRatioThreshold());
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
            return GalaxybaseConverter.getParams(
                operation.getSrcId(),
                operation.getDstId(),
                operation.getTime(),
                operation.getThreshold(),
                operation.getStartTime(),
                operation.getEndTime());
        }
    }
}
*/

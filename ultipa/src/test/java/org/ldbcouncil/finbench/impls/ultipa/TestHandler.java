package org.ldbcouncil.finbench.impls.ultipa;

import org.junit.Test;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class TestHandler {

    public UltipaDbConnectionState getState() throws DbException {
        Properties properties = new Properties();
        Map<String, String> map = new HashMap<>();

        try (FileInputStream fileInputStream = new FileInputStream("D:/javaworkspace/ldbc_finbench_transaction_impls/ultipa/src/main/resources/ultipa_example.properties")) {
            properties.load(fileInputStream);

            for (String key : properties.stringPropertyNames()) {
                String value = properties.getProperty(key);
                map.put(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        UltipaQueryStore queryStore = new UltipaQueryStore(properties.get("queryDir").toString());
        return new UltipaDbConnectionState(map,queryStore);
    }

    @Test
    public void TestComplexRead1Handler(){
        try{
            ComplexRead1 operation = new ComplexRead1(
                    79164837199875L,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead1Handler handler = new UltipaDb.ComplexRead1Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestComplexRead2Handler(){
        try{
            ComplexRead2 operation = new ComplexRead2(
                    2199023255552L,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead2Handler handler = new UltipaDb.ComplexRead2Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestComplexRead3Handler(){
        try{
            ComplexRead3 operation = new ComplexRead3(
                    52776558136573L,87960930225288L,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead3Handler handler = new UltipaDb.ComplexRead3Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestComplexRead4Handler(){
        try{
            ComplexRead4 operation = new ComplexRead4(
                    70368744180282L,70368744179862L,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead4Handler handler = new UltipaDb.ComplexRead4Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestComplexRead5Handler(){
        try{
            ComplexRead5 operation = new ComplexRead5(
                    2199023255552L,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead5Handler handler = new UltipaDb.ComplexRead5Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestComplexRead6Handler(){
        try{
            ComplexRead6 operation = new ComplexRead6(
                    114349209288720L,10,10,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead6Handler handler = new UltipaDb.ComplexRead6Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestComplexRead7Handler(){
        try{
            ComplexRead7 operation = new ComplexRead7(
                    114349209288720L,10,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead7Handler handler = new UltipaDb.ComplexRead7Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestComplexRead8Handler(){
        try{
            ComplexRead8 operation = new ComplexRead8(
                    123145302310912L,10,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead8Handler handler = new UltipaDb.ComplexRead8Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestComplexRead9Handler(){
        try{
            ComplexRead9 operation = new ComplexRead9(
                    96757023246377L,10,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead9Handler handler = new UltipaDb.ComplexRead9Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestComplexRead10Handler(){
        try{
            ComplexRead10 operation = new ComplexRead10(
                    13194139535301L,10995116279159L,new Date(1577836801000L),new Date(1672531210000L)
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead10Handler handler = new UltipaDb.ComplexRead10Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestComplexRead11Handler(){
        try{
            ComplexRead11 operation = new ComplexRead11(
                    10995116279159L,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead11Handler handler = new UltipaDb.ComplexRead11Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestComplexRead12Handler(){
        try{
            ComplexRead12 operation = new ComplexRead12(
                    10995116279159L,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ComplexRead12Handler handler = new UltipaDb.ComplexRead12Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestSimpleRead1Handler(){
        try{
            SimpleRead1 operation = new SimpleRead1(
                    96757023246377L
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.SimpleRead1Handler handler = new UltipaDb.SimpleRead1Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestSimpleRead2Handler(){
        try{
            SimpleRead2 operation = new SimpleRead2(
                    96757023246377L,new Date(1577836801000L),new Date(1672531210000L)
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.SimpleRead2Handler handler = new UltipaDb.SimpleRead2Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestSimpleRead3Handler(){
        try{
            SimpleRead3 operation = new SimpleRead3(
                    87960930225701L,10,new Date(1577836801000L),new Date(1672531210000L)
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.SimpleRead3Handler handler = new UltipaDb.SimpleRead3Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestSimpleRead4Handler(){
        try{
            SimpleRead4 operation = new SimpleRead4(
                    87960930225701L,10,new Date(1577836801000L),new Date(1672531210000L)
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.SimpleRead4Handler handler = new UltipaDb.SimpleRead4Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestSimpleRead5Handler(){
        try{
            SimpleRead5 operation = new SimpleRead5(
                    87960930225701L,10,new Date(1577836801000L),new Date(1672531210000L)
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.SimpleRead5Handler handler = new UltipaDb.SimpleRead5Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestSimpleRead6Handler(){
        try{
            SimpleRead6 operation = new SimpleRead6(
                    87960930225701L,new Date(1577836801000L),new Date(1672531210000L)
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.SimpleRead6Handler handler = new UltipaDb.SimpleRead6Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
            System.out.println(resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite1(){
        try{
            Write1 operation = new Write1(
                    1010101,"Person|1010101",2010101,new Date(1577836801000L),true,"card"
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write1Handler handler = new UltipaDb.Write1Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite2(){
        try{
            Write2 operation = new Write2(
                    3010101,"Company|3010101",2010102,new Date(1577836801000L),true,"card"
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write2Handler handler = new UltipaDb.Write2Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite3(){
        try{
            Write3 operation = new Write3(
                    2010101,2010102,new Date(1577536801000L),200.2
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write3Handler handler = new UltipaDb.Write3Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite4(){
        try{
            Write4 operation = new Write4(
                    2010102,2010101,new Date(1578536801000L),15
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write4Handler handler = new UltipaDb.Write4Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite5(){
        try{
            Write5 operation = new Write5(
                    1010101,new Date(1578436801000L),4010101,10
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write5Handler handler = new UltipaDb.Write5Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    @Test
    public void TestWrite6(){
        try{
            Write6 operation = new Write6(
                    3010101,new Date(1578536801000L),4010102,100.2
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write6Handler handler = new UltipaDb.Write6Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite7(){
        try{
            Write7 operation = new Write7(
                    2010102,5010101,true,new Date(1578546801000L)
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write7Handler handler = new UltipaDb.Write7Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite8(){
        try{
            Write8 operation = new Write8(
                    2010102,4010103,new Date(1578846801000L),10
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write8Handler handler = new UltipaDb.Write8Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite9(){
        try{
            Write9 operation = new Write9(
                    2010102,4010104,new Date(1578946801000L),5
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write9Handler handler = new UltipaDb.Write9Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite10(){
        try{
            Write10 operation = new Write10(
                    2010102
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write10Handler handler = new UltipaDb.Write10Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite11(){
        try{
            Write11 operation = new Write11(
                    1010101
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write11Handler handler = new UltipaDb.Write11Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite12(){
        try{
            Write12 operation = new Write12(
                    1010101,5,new Date(1578946801000L)
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write12Handler handler = new UltipaDb.Write12Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestWrite13(){
        try{
            Write13 operation = new Write13(
                    2010102
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.Write13Handler handler = new UltipaDb.Write13Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestReadWrite1(){
        try{
            ReadWrite1 operation = new ReadWrite1(
                    70368744180282L,70368744179862L,new Date(1578846801000L),10,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ReadWrite1Handler handler = new UltipaDb.ReadWrite1Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestReadWrite2(){
        try{
            ReadWrite2 operation = new ReadWrite2(
                    114349209288720L,35184372088833L,new Date(1578846801000L),10,10,new Date(1577836801000L),new Date(1672531210000L),2.0f,10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ReadWrite2Handler handler = new UltipaDb.ReadWrite2Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestReadWrite3(){
        try{
            ReadWrite3 operation = new ReadWrite3(
                    10995116279159L,26388279066626L,new Date(1577896801000L),10,new Date(1577836801000L),new Date(1672531210000L),10000, TruncationOrder.ASC
            );
            UltipaDbConnectionState state = getState();
            UltipaDb.ReadWrite3Handler handler = new UltipaDb.ReadWrite3Handler();
            ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
            handler.executeOperation(operation,state,resultReporter);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}

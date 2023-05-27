package org.ldbcouncil.finbench.impls.ultipa;

import org.junit.Before;
import org.junit.Test;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class TestRunHandler {

    public List<String> getQueryParam(String operationName) {
        String csvFile = "D:\\javaworkspace\\ldbc_finbench_transaction_impls\\ultipa\\src\\test\\queryParam\\"+operationName+".csv";
        List<String> list = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
            String line;
            boolean isFirstLine = true;
            while ((line = reader.readLine()) != null) {
                if(isFirstLine){
                    isFirstLine = false;
                    continue;
                }
                if(line.startsWith("##")){
                    continue;
                }

                list.add(line);
                //System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public TruncationOrder getOrder(String order){
        if("ASC".equals(order.toUpperCase())){
            return TruncationOrder.ASC;
        }else if("DESC".equals(order.toUpperCase())){
            return TruncationOrder.DESC;
        }else{
            return TruncationOrder.ASC;
        }
    }

    UltipaDbConnectionState state;
    @Before
    public  void getState() throws DbException {
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
        state = new UltipaDbConnectionState(map,queryStore);
    }

    @Test
    public void TestComplexRead1Handler(){
        System.out.println("--------------------------complex_read1  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read1");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read1  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead1 operation = new ComplexRead1(
                        Long.parseLong(ps[0]),new Date(Long.parseLong(ps[1])),new Date(Long.parseLong(ps[2])),Integer.parseInt(ps[3]), getOrder(ps[4])
                );
                UltipaDb.ComplexRead1Handler handler = new UltipaDb.ComplexRead1Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read1  end-----------------------------------------------------");
    }


    @Test
    public void TestComplexRead2Handler(){
        System.out.println("--------------------------complex_read2  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read2");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read2  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead2 operation = new ComplexRead2(
                        Long.parseLong(ps[0]),new Date(Long.parseLong(ps[1])),new Date(Long.parseLong(ps[2])),Integer.parseInt(ps[3]), getOrder(ps[4])
                );
                UltipaDb.ComplexRead2Handler handler = new UltipaDb.ComplexRead2Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read2  end-----------------------------------------------------");
    }

    @Test
    public void TestComplexRead3Handler(){
        System.out.println("--------------------------complex_read3  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read3");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read3  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead3 operation = new ComplexRead3(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),new Date(Long.parseLong(ps[2])),new Date(Long.parseLong(ps[3])),Integer.parseInt(ps[4]), getOrder(ps[5])
                );
                UltipaDb.ComplexRead3Handler handler = new UltipaDb.ComplexRead3Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read3  end-----------------------------------------------------");
    }

    @Test
    public void TestComplexRead4Handler(){
        System.out.println("--------------------------complex_read4  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read4");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read4  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead4 operation = new ComplexRead4(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),new Date(Long.parseLong(ps[2])),new Date(Long.parseLong(ps[3])),Integer.parseInt(ps[4]), getOrder(ps[5])
                );
                UltipaDb.ComplexRead4Handler handler = new UltipaDb.ComplexRead4Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read4  end-----------------------------------------------------");
    }

    @Test
    public void TestComplexRead5Handler(){
        System.out.println("--------------------------complex_read5  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read5");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read5  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead5 operation = new ComplexRead5(
                        Long.parseLong(ps[0]),new Date(Long.parseLong(ps[1])),new Date(Long.parseLong(ps[2])),Integer.parseInt(ps[3]), getOrder(ps[4])
                );
                UltipaDb.ComplexRead5Handler handler = new UltipaDb.ComplexRead5Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read5  end-----------------------------------------------------");
    }


    @Test
    public void TestComplexRead6Handler(){
        System.out.println("--------------------------complex_read6  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read6");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read6  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead6 operation = new ComplexRead6(
                        Long.parseLong(ps[0]),Double.parseDouble(ps[1]),Double.parseDouble(ps[2]),new Date(Long.parseLong(ps[3])),new Date(Long.parseLong(ps[4])),Integer.parseInt(ps[5]), getOrder(ps[6])
                );
                UltipaDb.ComplexRead6Handler handler = new UltipaDb.ComplexRead6Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read6  end-----------------------------------------------------");
    }

    @Test
    public void TestComplexRead7Handler(){
        System.out.println("--------------------------complex_read7  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read7");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read7  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead7 operation = new ComplexRead7(
                        Long.parseLong(ps[0]),Double.parseDouble(ps[1]),new Date(Long.parseLong(ps[2])),new Date(Long.parseLong(ps[3])),Integer.parseInt(ps[4]), getOrder(ps[5])
                );
                UltipaDb.ComplexRead7Handler handler = new UltipaDb.ComplexRead7Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read7  end-----------------------------------------------------");
    }

    @Test
    public void TestComplexRead8Handler(){
        System.out.println("--------------------------complex_read8  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read8");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read8  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead8 operation = new ComplexRead8(
                        Long.parseLong(ps[0]),Float.parseFloat(ps[1]),new Date(Long.parseLong(ps[2])),new Date(Long.parseLong(ps[3])),Integer.parseInt(ps[4]), getOrder(ps[5])
                );
                UltipaDb.ComplexRead8Handler handler = new UltipaDb.ComplexRead8Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read8  end-----------------------------------------------------");
    }

    @Test
    public void TestComplexRead9Handler(){
        System.out.println("--------------------------complex_read9  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read9");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read9  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead9 operation = new ComplexRead9(
                        Long.parseLong(ps[0]),Double.parseDouble(ps[1]),new Date(Long.parseLong(ps[2])),new Date(Long.parseLong(ps[3])),Integer.parseInt(ps[4]), getOrder(ps[5])
                );
                UltipaDb.ComplexRead9Handler handler = new UltipaDb.ComplexRead9Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read9  end-----------------------------------------------------");
    }

    @Test
    public void TestComplexRead10Handler(){
        System.out.println("--------------------------complex_read10  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read10");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read10  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead10 operation = new ComplexRead10(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),new Date(Long.parseLong(ps[2])),new Date(Long.parseLong(ps[3]))
                );
                UltipaDb.ComplexRead10Handler handler = new UltipaDb.ComplexRead10Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read10  end-----------------------------------------------------");
    }

    @Test
    public void TestComplexRead11Handler(){
        System.out.println("--------------------------complex_read11  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read11");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read11  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead11 operation = new ComplexRead11(
                        Long.parseLong(ps[0]),new Date(Long.parseLong(ps[1])),new Date(Long.parseLong(ps[2])),Integer.parseInt(ps[3]), getOrder(ps[4])
                );
                UltipaDb.ComplexRead11Handler handler = new UltipaDb.ComplexRead11Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read11  end-----------------------------------------------------");
    }

    @Test
    public void TestComplexRead12Handler(){
        System.out.println("--------------------------complex_read12  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("complex_read12");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------complex_read12  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ComplexRead12 operation = new ComplexRead12(
                        Long.parseLong(ps[0]),new Date(Long.parseLong(ps[1])),new Date(Long.parseLong(ps[2])),Integer.parseInt(ps[3]), getOrder(ps[4])
                );
                UltipaDb.ComplexRead12Handler handler = new UltipaDb.ComplexRead12Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------complex_read12  end-----------------------------------------------------");
    }

    @Test
    public void TestSimpleRead1Handler(){
        System.out.println("--------------------------simple_read1  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("simple_read1");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------simple_read1  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                SimpleRead1 operation = new SimpleRead1(
                        Long.parseLong(ps[0])
                );
                UltipaDb.SimpleRead1Handler handler = new UltipaDb.SimpleRead1Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------simple_read1  end-----------------------------------------------------");
    }

    @Test
    public void TestSimpleRead2Handler(){
        System.out.println("--------------------------simple_read2  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("simple_read2");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------simple_read2  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                SimpleRead2 operation = new SimpleRead2(
                        Long.parseLong(ps[0]),new Date(Long.parseLong(ps[1])),new Date(Long.parseLong(ps[2]))
                );
                UltipaDb.SimpleRead2Handler handler = new UltipaDb.SimpleRead2Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------simple_read2  end-----------------------------------------------------");
    }

    @Test
    public void TestSimpleRead3Handler(){
        System.out.println("--------------------------simple_read3  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("simple_read3");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------simple_read3  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                SimpleRead3 operation = new SimpleRead3(
                        Long.parseLong(ps[0]),Double.parseDouble(ps[1]),new Date(Long.parseLong(ps[2])),new Date(Long.parseLong(ps[3]))
                );
                UltipaDb.SimpleRead3Handler handler = new UltipaDb.SimpleRead3Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------simple_read3  end-----------------------------------------------------");
    }

    @Test
    public void TestSimpleRead4Handler(){
        System.out.println("--------------------------simple_read4  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("simple_read4");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------simple_read4  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                SimpleRead4 operation = new SimpleRead4(
                        Long.parseLong(ps[0]),Double.parseDouble(ps[1]),new Date(Long.parseLong(ps[2])),new Date(Long.parseLong(ps[3]))
                );
                UltipaDb.SimpleRead4Handler handler = new UltipaDb.SimpleRead4Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------simple_read4  end-----------------------------------------------------");
    }

    @Test
    public void TestSimpleRead5Handler(){
        System.out.println("--------------------------simple_read5  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("simple_read5");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------simple_read5  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                SimpleRead5 operation = new SimpleRead5(
                        Long.parseLong(ps[0]),Double.parseDouble(ps[1]),new Date(Long.parseLong(ps[2])),new Date(Long.parseLong(ps[3]))
                );
                UltipaDb.SimpleRead5Handler handler = new UltipaDb.SimpleRead5Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------simple_read5  end-----------------------------------------------------");
    }

    @Test
    public void TestSimpleRead6Handler(){
        System.out.println("--------------------------simple_read6  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("simple_read6");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------simple_read6  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                SimpleRead6 operation = new SimpleRead6(
                        Long.parseLong(ps[0]),new Date(Long.parseLong(ps[1])),new Date(Long.parseLong(ps[2]))
                );
                UltipaDb.SimpleRead6Handler handler = new UltipaDb.SimpleRead6Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------simple_read6  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite1Handler(){
        System.out.println("--------------------------write1  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write1");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write1  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write1 operation = new Write1(
                        Long.parseLong(ps[0]),ps[1],Long.parseLong(ps[2]),new Date(Long.parseLong(ps[3])),Boolean.parseBoolean(ps[4]),ps[5]
                );
                UltipaDb.Write1Handler handler = new UltipaDb.Write1Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write1  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite2Handler(){
        System.out.println("--------------------------write2  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write2");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write2  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write2 operation = new Write2(
                        Long.parseLong(ps[0]),ps[1],Long.parseLong(ps[2]),new Date(Long.parseLong(ps[3])),Boolean.parseBoolean(ps[4]),ps[5]
                );
                UltipaDb.Write2Handler handler = new UltipaDb.Write2Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write2  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite3Handler(){
        System.out.println("--------------------------write3  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write3");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write3  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write3 operation = new Write3(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),new Date(Long.parseLong(ps[2])),Double.parseDouble(ps[3])
                );
                UltipaDb.Write3Handler handler = new UltipaDb.Write3Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write3  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite4Handler(){
        System.out.println("--------------------------write4  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write4");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write4  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write4 operation = new Write4(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),new Date(Long.parseLong(ps[2])),Double.parseDouble(ps[3])
                );
                UltipaDb.Write4Handler handler = new UltipaDb.Write4Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write4  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite5Handler(){
        System.out.println("--------------------------write5  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write5");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write5  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write5 operation = new Write5(
                        Long.parseLong(ps[0]),new Date(Long.parseLong(ps[1])),Long.parseLong(ps[2]),Double.parseDouble(ps[3])
                );
                UltipaDb.Write5Handler handler = new UltipaDb.Write5Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write5  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite6Handler(){
        System.out.println("--------------------------write6  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write6");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write6  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write6 operation = new Write6(
                        Long.parseLong(ps[0]),new Date(Long.parseLong(ps[1])),Long.parseLong(ps[2]),Double.parseDouble(ps[3])
                );
                UltipaDb.Write6Handler handler = new UltipaDb.Write6Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write6  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite7Handler(){
        System.out.println("--------------------------write7  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write7");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write7  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write7 operation = new Write7(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),Boolean.parseBoolean(ps[2]),new Date(Long.parseLong(ps[3]))
                );
                UltipaDb.Write7Handler handler = new UltipaDb.Write7Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write7  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite8Handler(){
        System.out.println("--------------------------write8  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write8");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write8  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write8 operation = new Write8(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),new Date(Long.parseLong(ps[2])),Double.parseDouble(ps[3])
                );
                UltipaDb.Write8Handler handler = new UltipaDb.Write8Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write8  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite9Handler(){
        System.out.println("--------------------------write9  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write9");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write9  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write9 operation = new Write9(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),new Date(Long.parseLong(ps[2])),Double.parseDouble(ps[3])
                );
                UltipaDb.Write9Handler handler = new UltipaDb.Write9Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write9  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite10Handler(){
        System.out.println("--------------------------write10  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write10");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write10  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write10 operation = new Write10(
                        Long.parseLong(ps[0])
                );
                UltipaDb.Write10Handler handler = new UltipaDb.Write10Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write10  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite11Handler(){
        System.out.println("--------------------------write11  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write11");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write11  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write11 operation = new Write11(
                        Long.parseLong(ps[0])
                );
                UltipaDb.Write11Handler handler = new UltipaDb.Write11Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write11  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite12Handler(){
        System.out.println("--------------------------write12  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write12");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write12  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write12 operation = new Write12(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),new Date(Long.parseLong(ps[2]))
                );
                UltipaDb.Write12Handler handler = new UltipaDb.Write12Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write12  end-----------------------------------------------------");
    }

    @Test
    public void TestWrite13Handler(){
        System.out.println("--------------------------write13  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("write13");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------write13  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                Write13 operation = new Write13(
                        Long.parseLong(ps[0])
                );
                UltipaDb.Write13Handler handler = new UltipaDb.Write13Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------write13  end-----------------------------------------------------");
    }

    @Test
    public void TestReadWrite1Handler(){
        System.out.println("--------------------------read_write1  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("read_write1");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------read_write1  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ReadWrite1 operation = new ReadWrite1(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),new Date(Long.parseLong(ps[2])),Double.parseDouble(ps[3]),new Date(Long.parseLong(ps[4])),new Date(Long.parseLong(ps[5])),Integer.parseInt(ps[6]), getOrder(ps[7])
                );
                UltipaDb.ReadWrite1Handler handler = new UltipaDb.ReadWrite1Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------read_write1  end-----------------------------------------------------");
    }

    @Test
    public void TestReadWrite2Handler(){
        System.out.println("--------------------------read_write2  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("read_write2");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------read_write2  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ReadWrite2 operation = new ReadWrite2(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),new Date(Long.parseLong(ps[2])),Double.parseDouble(ps[3]),Double.parseDouble(ps[4]),new Date(Long.parseLong(ps[5])),new Date(Long.parseLong(ps[6])),Float.parseFloat(ps[7]),Integer.parseInt(ps[8]), getOrder(ps[9])
                );
                UltipaDb.ReadWrite2Handler handler = new UltipaDb.ReadWrite2Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------read_write2  end-----------------------------------------------------");
    }

    @Test
    public void TestReadWrite3Handler(){
        System.out.println("--------------------------read_write3  start-----------------------------------------------------");
        List<String> paramList = getQueryParam("read_write3");
        for (int i = 0; i < paramList.size(); i++) {
            System.out.println("------------read_write3  execute:"+(i+1)+"--------------------");
            String Params = paramList.get(i);
            System.out.println(Params);
            String[] ps = Params.split(",");
            try{
                ReadWrite3 operation = new ReadWrite3(
                        Long.parseLong(ps[0]),Long.parseLong(ps[1]),new Date(Long.parseLong(ps[2])),Double.parseDouble(ps[3]),new Date(Long.parseLong(ps[4])),new Date(Long.parseLong(ps[5])),Integer.parseInt(ps[6]), getOrder(ps[7])
                );
                UltipaDb.ReadWrite3Handler handler = new UltipaDb.ReadWrite3Handler();
                ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(null);
                handler.executeOperation(operation,state,resultReporter);
                System.out.println(resultReporter);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("--------------------------read_write3  end-----------------------------------------------------");
    }
}

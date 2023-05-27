package org.ldbcouncil.finbench.impls.ultipa;

import com.ultipa.sdk.connect.Connection;
import com.ultipa.sdk.operate.entity.Attr;
import com.ultipa.sdk.operate.entity.DataItem;
import com.ultipa.sdk.operate.entity.Node;
import com.ultipa.sdk.operate.response.Response;
import org.junit.Test;
import org.ldbcouncil.finbench.driver.DbException;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

public class TestDbConn {
    @Test
    public void getTimeStampe(){
       System.out.println(Timestamp.valueOf("2020-01-01 08:00:01").getTime());
       System.out.println(Timestamp.valueOf("2023-01-01 08:00:10").getTime());
    }

    @Test
    public void testUql(){
        try {
            UltipaDbConnectionState state = getState();
            Connection conn = state.getConn();

            String uql = "n({@Person && _id=='Person|2199023255552'}).re({@Own}).n({@Account} as account)\n" +
                    "exta(complex_read).params({\n" +
                    "\tsrcs:account,\n" +
                    "\tedgeDirection:\"out\",\n" +
                    "\tdepth:3,\n" +
                    "\tlimit:10000000,\n" +
                    "\tedge_schema:\"Transfer\",\n" +
                    "\tedge_timestamp:\"timestamp\",\n" +
                    "\tedge_amount:\"amount\",\n" +
                    "    amount:-1,\n" +
                    "    pre:1,\n" +
                    "\trange_begin:\"2020-01-01 08:00:01\",\n" +
                    "\trange_end:\"2023-01-01 08:00:10\",\n" +
                    "\tnoCircle:1,\n" +
                    "\ttop_n:10000,\n" +
                    "\torder:\"asc\",\n" +
                    "\treturn_type:0\n" +
                    "}).stream() as rest\n" +
                    "return rest";

            Response resp = conn.uql(uql);
            Map<String, DataItem> map = resp.getItems();
            Attr attr = map.get("rest").asAttr();
            List<Object> list = attr.getValues();
            for(Object o : list){
                System.out.println("------------------path-----------------");
                List<Node> nodes = (List<Node>) o;
                for(Node node:nodes){
                    Response resp2 = conn.uql("find().nodes("+node.getUUID()+") as node return node{*}");
                    System.out.println(resp2.get(0).asNodes().get(0).getID());
                }

            }

        } catch (DbException e) {
            throw new RuntimeException(e);
        }


    }

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


}

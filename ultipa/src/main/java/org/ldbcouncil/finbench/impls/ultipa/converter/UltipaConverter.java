package org.ldbcouncil.finbench.impls.ultipa.converter;

import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

public  class UltipaConverter {
    final static String DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";

    public static String convertDate(Date date) {
        final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        //sdf.setTimeZone(TimeZone.getTimeZone("Etc/GMT+0"));
        return "'" + sdf.format(date) + "'";
    }

    public static String converOrder(TruncationOrder order){
        if(order == TruncationOrder.ASC){
            return "'asc'";
        }else{
            return "'desc'";
        }
    }

    public static String convertString(String value) {
        return "'" + value.replace("'", "\\'") + "'";
    }

    public static String convertInteger(int value) {
        return Integer.toString(value);
    }


    public static String convertId(long value) {
        return "'" + value + "'";
    }

    public static String convertIdStr(Object value,String suf) {
        return "'" + suf + value + "'";
    }

    public static String convertLong(long value) {
        return Long.toString(value);
    }

    public static String convertFloat(float value) {
        return Float.toString(value);
    }

    public static String convertDouble(double value) {
        return Double.toString(value);
    }
    public static String convertBool(boolean value){
        return "'" + Boolean.toString(value) +"'";
    }

    public static String replaceVariables(String input, Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String variable = "\\$" + key;

            if (value != null) {
                input = input.replaceAll(variable, value.toString());
            } else {
                input = input.replaceAll(variable, "");
            }
        }
        return input;
    }
}

package org.ldbcouncil.finbench.impls.gpstore.converter;

import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;

import java.util.Date;

public class GpstoreConverter {
    private final static String COLLECTION_SEPARATOR = ", ";

    public String convertDate(Date date) {
        return String.valueOf(date.getTime());
    }

    public String convertDateTime(Date date) {
        return String.valueOf(date.getTime());
    }

    public static String convertOrderWithQuotes(TruncationOrder order){
        if(order == TruncationOrder.TIMESTAMP_ASCENDING){
            return "'asc'";
        }else{
            return "'desc'";
        }
    }

    public static String convertOrder(TruncationOrder order){
        if(order == TruncationOrder.TIMESTAMP_ASCENDING){
            return "1";
        }else{
            return "0";
        }
    }

    public static String convertLong(long value) {
        return Long.toString(value);
    }

    public static String convertInt(int value) {
        return Integer.toString(value);
    }


    public static String convertDouble(double value) {
        return Double.toString(value);
    }

    public static String convertFloat(float value) {
        return Float.toString(value);
    }

    public static String convertString(String value) {
        return "'" + value.replace("'", "\\'") + "'";
    }

    public static String convertBool(boolean value){
        return "'" + Boolean.toString(value) +"'";
    }
}

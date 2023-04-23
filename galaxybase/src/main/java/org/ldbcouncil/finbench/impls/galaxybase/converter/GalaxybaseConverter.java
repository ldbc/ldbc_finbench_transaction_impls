package org.ldbcouncil.finbench.impls.galaxybase.converter;


import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GalaxybaseConverter {

    public static String convertString(String value) {
        try {
            return URLEncoder.encode(value, "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String convertLongList(List<Long> values) {
        String res = values
            .stream()
            .map(Object::toString)
            .collect(Collectors.joining(";"));
        return res;
    }

    public static String convertStringList(List<String> values) {
        return String.join(";", values);
    }

    public static List<String> convertStringToList(String content) {
        return Arrays.asList(content.split(";"));
    }

    public static String getParams(Object... list) {
        StringBuilder builder = new StringBuilder();
        for (Object value : list) {
            builder.append(value).append("|");
        }
        return builder.toString();
    }

    public static String[] getValues(String value) {
        return value.split("\\|", 20);
    }
}

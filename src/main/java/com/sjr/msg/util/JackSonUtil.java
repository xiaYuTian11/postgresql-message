package com.sjr.msg.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author 毛文超
 * @date 2019/4/159:09 AM
 */
public class JackSonUtil {

    public static final ObjectMapper JSON = new ObjectMapper();

    static {
        JSON.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String toJson(Object o) {

        try {
            return JSON.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "";
        }
    }

    public static <T> T toObject(String str, Class<T> cs) {
        try {
            return JSON.readValue(str, cs);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

}

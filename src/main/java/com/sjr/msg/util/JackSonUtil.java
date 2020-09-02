package com.sjr.msg.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

/**
 * json 转换
 *
 * @author TMW
 * @date 2019/8/15
 */
@Slf4j
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
            log.error("toJson 转换异常", e);
            return "";
        }
    }

    public static <T> T toObject(String str, Class<T> cs) {
        try {
            return JSON.readValue(str, cs);
        } catch (JsonProcessingException e) {
            log.error("toObject 转换异常", e);
        }
        return null;
    }

}

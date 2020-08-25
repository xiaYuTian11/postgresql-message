package com.sjr.msg.biz.service;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;

public class DecodeUtils {

    private DecodeUtils() {
    }

    public static String readColumnValueAsString(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] value = new byte[length];
        buffer.get(value, 0, length);
        return new String(value, Charset.forName("UTF-8"));
    }

    public static String readString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        byte b = 0;
        while ((b = buffer.get()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }

    public static String unquoteIdentifierPart(String identifierPart) {
        if (identifierPart == null || identifierPart.length() < 2) {
            return identifierPart;
        }

        Character quotingChar = deriveQuotingChar(identifierPart);
        if (quotingChar != null) {
            identifierPart = identifierPart.substring(1, identifierPart.length() - 1);
            identifierPart = identifierPart.replace(quotingChar.toString() + quotingChar.toString(), quotingChar.toString());
        }

        return identifierPart;
    }

    public static Character deriveQuotingChar(String identifierPart) {
        char first = identifierPart.charAt(0);
        char last = identifierPart.charAt(identifierPart.length() - 1);

        if (first == last && (first == '"' || first == '\'' || first == '`')) {
            return first;
        }

        return null;
    }

    public static HashMap<String, String> resolveColumnsFromStreamTupleData(ByteBuffer buffer) {
        short numberOfColumns = buffer.getShort();
        HashMap<String, String> map = new HashMap<>(numberOfColumns);
        for (int i = 0; i < numberOfColumns; i++) {
            char type = (char) buffer.get();
            String value = null;
            if (type == 't') {
                value = readColumnValueAsString(buffer);
            } else if (type == 'u') {
                value = null;
            } else if (type == 'n') {
                value = null;
            }
            map.put(value, value);
        }
        return map;
    }

}

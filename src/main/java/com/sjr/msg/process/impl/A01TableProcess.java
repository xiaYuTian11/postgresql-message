package com.sjr.msg.process.impl;

import com.sjr.msg.process.TableProcess;

import java.util.HashSet;
import java.util.Set;

/**
 * a01
 *
 * @author TMW
 * @date 2020/9/1 10:10
 */
public class A01TableProcess implements TableProcess {

    /**
     * 需要同步的字段
     *
     * @return
     */
    @Override
    public Set<String> getSyncFields() {
        return new HashSet<String>(16) {{
            add("A0000");
            add("A0101");
            add("A0102");
            add("A0117");
            add("A0134");
            add("A0141");
            add("A0184");
            add("A0104");
            add("A0163");
            add("A0195");
            add("ZZXL");
            add("A0111A");
            add("A0192A");
            add("A0107");
            add("A0221");
            add("A0114A");
            add("A0140");
            add("A0128");
            add("A0187A");
            add("A0197");
            add("A0165");
            add("A0160");
            add("A0121");
            add("A0288");
            add("A2949");
            add("A0155A");
            add("A1701");
            add("A0198");
        }};
    }

}

package com.sjr.msg.process.impl;

import com.sjr.msg.process.TableProcess;

import java.util.HashSet;
import java.util.Set;

/**
 * a08
 *
 * @author TMW
 * @date 2020/9/1 14:14
 */
public class A08TableProcess implements TableProcess {

    /**
     * 需要同步的字段
     *
     * @return
     */
    @Override
    public Set<String> getSyncFields() {
        return new HashSet<String>(16) {{
            add("A0000");
            add("A0801A");
            add("A0801B");
            add("A0804");
            add("A0807");
            add("A0814");
            add("A0824");
            add("A0827");
            add("A0901A");
            add("A0901B");
        }};
    }

}

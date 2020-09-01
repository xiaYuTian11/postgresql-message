package com.sjr.msg.process.impl;

import com.sjr.msg.process.TableProcess;

import java.util.HashSet;
import java.util.Set;

/**
 * a37
 *
 * @author TMW
 * @date 2020/9/1 14:14
 */
public class A37TableProcess implements TableProcess {

    /**
     * 需要同步的字段
     *
     * @return
     */
    @Override
    public Set<String> getSyncFields() {
        return new HashSet<String>(16) {{
            add("A0000");
            add("A3701");
            add("A3707A");
            add("A3707B");
            add("A3708");
        }};
    }

}

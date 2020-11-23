package com.sunnyday.msg.process.impl;

import com.sunnyday.msg.process.TableProcess;

import java.util.HashSet;
import java.util.Set;

/**
 * a06
 *
 * @author TMW
 * @date 2020/9/1 14:14
 */
public class A06TableProcess implements TableProcess {

    /**
     * 需要同步的字段
     *
     * @return
     */
    @Override
    public Set<String> getSyncFields() {
        return new HashSet<String>(16) {{
            add("A0000");
            add("A0602");
            add("A0601");
            add("A0604");
            add("A0607");
            add("A0611");
        }};
    }

}

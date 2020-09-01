package com.sjr.msg.process.impl;

import com.sjr.msg.process.TableProcess;

import java.util.HashSet;
import java.util.Set;

/**
 * a30
 *
 * @author TMW
 * @date 2020/9/1 14:14
 */
public class A30TableProcess implements TableProcess {

    /**
     * 需要同步的字段
     *
     * @return
     */
    @Override
    public Set<String> getSyncFields() {
        return new HashSet<String>(16) {{
            add("A0000");
            add("A3001");
            add("A3004");
            add("A3008");
            add("A3007A");
        }};
    }

}

package com.sjr.msg.process.impl;

import com.sjr.msg.process.TableProcess;

import java.util.HashSet;
import java.util.Set;

/**
 * a15
 *
 * @author TMW
 * @date 2020/9/1 14:14
 */
public class A15TableProcess implements TableProcess {

    /**
     * 需要同步的字段
     *
     * @return
     */
    @Override
    public Set<String> getSyncFields() {
        return new HashSet<String>(16) {{
            add("A0000");
            add("A1521");
            add("A1517");
        }};
    }

}

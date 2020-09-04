package com.sjr.msg.process.impl;

import com.sjr.msg.process.TableProcess;

import java.util.HashSet;
import java.util.Set;

/**
 * b01
 *
 * @author TMW
 * @date 2020/9/1 14:14
 */
public class B01TableProcess implements TableProcess {

    /**
     * 需要同步的字段
     *
     * @return
     */
    @Override
    public Set<String> getSyncFields() {
        return new HashSet<String>(16) {{
            add("id");
            add("B0100");
            add("B0101");
            add("USED");
        }};
    }

}

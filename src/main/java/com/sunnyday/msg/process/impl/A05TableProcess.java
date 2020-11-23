package com.sunnyday.msg.process.impl;

import com.sunnyday.msg.process.TableProcess;

import java.util.HashSet;
import java.util.Set;

/**
 * a05
 *
 * @author TMW
 * @date 2020/9/1 14:14
 */
public class A05TableProcess implements TableProcess {

    /**
     * 需要同步的字段
     *
     * @return
     */
    @Override
    public Set<String> getSyncFields() {
        return new HashSet<String>(16) {{
            add("A0000");
            add("A0511");
            add("A0504");
            add("A0501B");
            add("A0517");
        }};
    }

}

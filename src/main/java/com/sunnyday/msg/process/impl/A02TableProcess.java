package com.sunnyday.msg.process.impl;

import com.sunnyday.msg.process.TableProcess;

import java.util.HashSet;
import java.util.Set;

/**
 * a02
 *
 * @author TMW
 * @date 2020/9/1 10:10
 */
public class A02TableProcess implements TableProcess {

    /**
     * 需要同步的字段
     *
     * @return
     */
    @Override
    public Set<String> getSyncFields() {
        return new HashSet<String>(16) {{
          add("A0000");
          add("A0201A");
          add("A0201B");
          add("A0215A");
          add("A0201D");
          add("A0243");
          add("A0245");
          add("A0215B");
          add("A0265");
          add("A0267");
          add("A0229");
        }};
    }

}

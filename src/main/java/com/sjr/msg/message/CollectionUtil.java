package com.sjr.msg.message;

import java.util.Map;

/**
 * @author TMW
 * @date 2019/8/30 14:55
 */
public class CollectionUtil {

    /**
     * 比较两个map是否不同
     *
     * @param map1
     * @param map2
     * @return true 不等，false 相等
     */
    public static boolean compareMapNe(Map<String, String> map1, Map<String, String> map2) {
        boolean flag = false;
        for (Map.Entry<String, String> entry : map1.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            Object oldValue = map2.get(key);
            if (value == null && oldValue != null) {
                flag = true;
                break;
            }
            if (value != null && !value.equals(oldValue)) {
                flag = true;
                break;
            }
        }
        return flag;
    }

}

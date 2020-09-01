package com.sjr.msg.process;

import java.util.Objects;

/**
 * @author TMW
 * @date 2020/9/1 9:59
 */
@SuppressWarnings("all")
public enum TableType {
    A01("a01", "基本情况信息集"),
    A02("a02", "任职机构信息"),
    A05("a05", "职务职级信息集"),
    A06("a06", "专业技术任职资格信息集"),
    A08("a06", "职务职级信息集"),
    A14("a14", "奖惩信息集"),
    A15("a15", "考核结论"),
    A29("a29", "进入本单位信息"),
    A30("a30", "退出本单位变动类别"),
    A36("a36", "家庭成员及社会关系信息集"),
    A37("a37", "住址通信信息集"),
    B01("b01", "单位基本情况信息");

    TableType(String name, String desc) {
        this.name = name;
        this.desc = desc;
    }

    private String name;
    private String desc;

    public String getName() {
        return name;
    }

    public TableType setName(String name) {
        this.name = name;
        return this;
    }

    public String getDesc() {
        return desc;
    }

    public TableType setDesc(String desc) {
        this.desc = desc;
        return this;
    }

    public static TableType forType(String tableName) {
        for (TableType tableType : TableType.values()) {
            if (Objects.equals(tableType.getName(), tableName)) {
                return tableType;
            }
        }
        return null;
    }

}

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.table.data.RowData;

public class RowDataWithKey {
    public RowDataWithKey() {

    }
    public String key;
    public RowData value;
    RowDataWithKey(String key, RowData value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public RowData getValue() {
        return value;
    }

    public void setValue(RowData value) {
        this.value = value;
    }
}

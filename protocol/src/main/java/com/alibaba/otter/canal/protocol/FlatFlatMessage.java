package com.alibaba.otter.canal.protocol;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.*;

/**
 * @author machengyuan 2018-9-13 下午10:31:14
 * @version 1.0.0
 */
public class FlatFlatMessage implements Serializable {

    private static final long         serialVersionUID = -3326650178737860050L;
    private long                      id;
    private String                    logFileName;
    private long                      logfileOffset;
    private String                    database;
    private String                    table;
    private List<String>              pkNames;
    private Boolean                   isDdl;
    private String                    optType;
    // binlog executeTime
    private Long                      es;
    // dml build timeStamp
    private Long                      ts;
    private String                    sql;
    private Map<String, Integer>      sqlType;
    private Map<String, String>       mysqlType;
    private Map<String, String> data;
    private Map<String, String> old;

    public FlatFlatMessage() {
    }

    public FlatFlatMessage(long id){
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void addPkName(String pkName) {
        if (this.pkNames == null) {
            this.pkNames = Lists.newArrayList();
        }
        this.pkNames.add(pkName);
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Boolean getIsDdl() {
        return isDdl;
    }

    public void setIsDdl(Boolean isDdl) {
        this.isDdl = isDdl;
    }

    public String getOptType() {
        return optType;
    }

    public FlatFlatMessage setOptType(String optType) {
        this.optType = optType;
        return this;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<String, Integer> getSqlType() {
        return sqlType;
    }

    public void setSqlType(Map<String, Integer> sqlType) {
        this.sqlType = sqlType;
    }

    public Map<String, String> getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(Map<String, String> mysqlType) {
        this.mysqlType = mysqlType;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

    public Map<String, String> getOld() {
        return old;
    }

    public void setOld(Map<String, String> old) {
        this.old = old;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public FlatFlatMessage setLogFileName(String logFileName) {
        this.logFileName = logFileName;
        return this;
    }

    public long getLogfileOffset() {
        return logfileOffset;
    }

    public FlatFlatMessage setLogfileOffset(long logfileOffset) {
        this.logfileOffset = logfileOffset;
        return this;
    }

    @Override
    public String toString() {
        return "FlatMessage [id=" + id + ", database=" + database + ", table=" + table + ", pkNames=" + pkNames + ", isDdl=" + isDdl + ", optType="
                + optType + ", es=" + es + ", ts=" + ts + ", sql=" + sql + ", sqlType=" + sqlType + ", mysqlType="
                + mysqlType + ", data=" + data + ", old=" + old + "]";
    }
}

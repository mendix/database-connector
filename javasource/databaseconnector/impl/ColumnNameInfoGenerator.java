package databaseconnector.impl;

import java.sql.ResultSetMetaData;

class ColumnNameInfoGenerator {
  private ResultSetMetaData resultSetMetaData;

  ColumnNameInfoGenerator(ResultSetMetaData resultSetMetaData) {
    this.resultSetMetaData = resultSetMetaData;
  }

  ColumnInfo getColumnInfo(int index) {
    try {
      return new ColumnInfo(index, resultSetMetaData.getColumnName(index));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
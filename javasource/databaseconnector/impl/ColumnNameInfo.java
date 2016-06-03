package databaseconnector.impl;

import java.sql.ResultSetMetaData;

class ColumnNameInfo {
  private ResultSetMetaData resultSetMetaData;

  ColumnNameInfo(ResultSetMetaData resultSetMetaData) {
    this.resultSetMetaData = resultSetMetaData;
  }

  String getColumnName(int index) {
    try {
      return resultSetMetaData.getColumnName(index);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
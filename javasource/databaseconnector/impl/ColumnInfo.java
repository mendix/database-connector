package databaseconnector.impl;

public class ColumnInfo {
  private int index;
  private String name;

  public ColumnInfo(int index, String name) {
    this.index = index;
    this.name = name;
  }

  public int getIndex() {
    return index;
  }

  public String getName() {
    return name;
  }
}

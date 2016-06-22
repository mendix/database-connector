package databaseconnector.impl;

import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

public class ColumnInfo {
  private int index;
  private String name;
  private PrimitiveType type;

  public ColumnInfo(int index, String name, PrimitiveType type) {
    this.index = index;
    this.name = name;
    this.type = type;
  }

  public int getIndex() {
    return index;
  }

  public String getName() {
    return name;
  }

  public PrimitiveType getType() {
    return type;
  }
}

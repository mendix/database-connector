package databaseconnector.impl;

import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

public class ColumnInfo {
  private final int index;
  private final String name;
  private final PrimitiveType type;

  public ColumnInfo(final int index, final String name, final PrimitiveType type) {
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

package databaseconnector.interfaces;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public interface ObjectInstantiator {
  IMendixObject instantiate(IContext context, String entityName);
}

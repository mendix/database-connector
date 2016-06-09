package databaseconnector.interfaces;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public interface IObjectInstantiator {
  IMendixObject instantiate(IContext context, String entityName);
}

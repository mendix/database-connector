package databaseconnector.impl;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.interfaces.IObjectInstantiator;

class ObjectInstantiatorImpl implements IObjectInstantiator {

  @Override
  public IMendixObject instantiate(IContext context, String entityName) {
    return Core.instantiate(context, entityName);
  }
}
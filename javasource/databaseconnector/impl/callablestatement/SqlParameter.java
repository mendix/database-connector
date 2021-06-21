package databaseconnector.impl.callablestatement;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterDatetime;
import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterList;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterObject;
import databaseconnector.proxies.ParameterString;

public abstract class SqlParameter implements Comparable<SqlParameter> {
	protected Parameter parameterObject;
	
	protected SqlParameter(final IContext context, final IMendixObject mendixParameterObject) {
		this.parameterObject = Parameter.initialize(context, mendixParameterObject);
		
		if ((this.parameterObject.getName() == null || this.parameterObject.getName().isBlank()) && this.parameterObject.getPosition() == null) {
			throw new IllegalArgumentException("Parameter was initialized with neither name or position.");
		}
	}

	public static SqlParameter initialize(final IContext context, IMendixObject mendixObject) {
		SqlParameter ret = null;

		switch (mendixObject.getType()) {
		case ParameterDatetime.entityName:
			ret = new SqlParameterDatetime(context, mendixObject);
			break;
		case ParameterString.entityName:
			ret = new SqlParameterString(context, mendixObject);
			break;
		case ParameterLong.entityName:
			ret = new SqlParameterLong(context, mendixObject);
			break;
		case ParameterDecimal.entityName:
			ret = new SqlParameterDecimal(context, mendixObject);
			break;
		case ParameterObject.entityName:
			List<SqlParameterPrimitiveValue> fields = Core
					.retrieveByPath(context, mendixObject, Parameter.MemberNames.MemberOfObject.toString(), true)
					.stream().map(p -> (SqlParameterPrimitiveValue) initialize(context, p)).sorted()
					.collect(Collectors.toList());
			ret = new SqlParameterObject(context, mendixObject, fields);
			break;
		case ParameterList.entityName:
			List<SqlParameter> elements = Core
					.retrieveByPath(context, mendixObject, Parameter.MemberNames.MemberOfList.toString(), true)
					.stream().map(p -> (SqlParameter) initialize(context, p)).sorted()
					.collect(Collectors.toList());
			ret = new SqlParameterList(context, mendixObject, elements);
			break;
		default:
			throw new IllegalArgumentException(
					String.format("Parameter type %s not supported.", mendixObject.getType()));
		}
		return ret;
	}
	

	public void prepareCall(CallableStatement cStatement) throws SQLException {
		switch (this.getParameterMode()) {
		case INPUT:
			prepareInput(cStatement);
			break;
		case OUTPUT:
			prepareOutput(cStatement);
			break;
		case INOUT:
			prepareInput(cStatement);
			prepareOutput(cStatement);
			break;
		default:
			throw new IllegalArgumentException(
					"Unrecognized parameter type" + this.getParameterMode().toString());
		}
	}

	public void retrieveResult(CallableStatement cStatement) throws SQLException, DatabaseConnectorException {
		if (this.getParameterMode().equals(ParameterMode.OUTPUT) || this.getParameterMode().equals(ParameterMode.INOUT)) {
			getValueOutput(cStatement);
		}
	}
	
	protected abstract void getValueOutput(CallableStatement cStatement) throws SQLException, DatabaseConnectorException;
	protected abstract void prepareOutput(CallableStatement cStatement) throws SQLException;
	protected abstract void prepareInput(CallableStatement cStatement) throws SQLException;

	abstract Object getMxObjectValue();
	abstract void setMxObjectValue(Object value) throws DatabaseConnectorException;
	
	public Integer getPosition() {
		return this.parameterObject.getPosition();
	}
	
	public String getName() {
		return this.parameterObject.getName();
	}

	public ParameterMode getParameterMode() {
		return this.parameterObject.getParameterMode();
	}

	@Override
	public int compareTo(SqlParameter other) {
		return this.getPosition() - other.getPosition();
	}

	public static SqlParameter createParameterFromValue(IContext context, ParameterMode mode, int index, Object value) throws DatabaseConnectorException {
		final String objectType;
		if (value instanceof Long) {
			objectType = ParameterLong.getType();
		} else if (value instanceof BigDecimal) {
			objectType = ParameterDecimal.getType();
		} else if (value instanceof String) {
			objectType = ParameterString.getType();
		} else if (value instanceof Date) {
			objectType = ParameterDatetime.getType();
		} else {
			throw new DatabaseConnectorException(String.format("Unable to infer data type from value '%s'.", value.toString()));
		}
			
		IMendixObject newObject = Core.instantiate(context, objectType);
		newObject.setValue(context, Parameter.MemberNames.ParameterMode.toString(), mode.toString());
		newObject.setValue(context, Parameter.MemberNames.Position.toString(), index);
		SqlParameterPrimitiveValue newValue = (SqlParameterPrimitiveValue) SqlParameter.initialize(context, newObject);
		newValue.setMxObjectValue(value);
		return newValue;
	}
}

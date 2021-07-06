package databaseconnector.impl.callablestatement;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Struct;
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
import databaseconnector.proxies.ParameterRefCursor;
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
			List<SqlParameter> fields = Core
					.retrieveByPath(context, mendixObject, ParameterObject.MemberNames.ParameterObject_Parameter.toString())
					.stream().map(p -> initialize(context, p)).sorted()
					.collect(Collectors.toList());
			ret = new SqlParameterObject(context, mendixObject, fields);
			break;
		case ParameterList.entityName:
			List<SqlParameter> elements = Core
					.retrieveByPath(context, mendixObject, ParameterList.MemberNames.ParameterList_Parameter.toString())
					.stream().map(p -> initialize(context, p)).sorted()
					.collect(Collectors.toList());
			ret = new SqlParameterList(context, mendixObject, elements);
			break;
		case ParameterRefCursor.entityName:
			ret = new SqlParameterRefCursor(context, mendixObject);
			break;
		default:
			throw new IllegalArgumentException(
					String.format("Parameter type %s not supported.", mendixObject.getType()));
		}
		return ret;
	}

	public void prepareCall(CallableStatement cStatement) throws SQLException, DatabaseConnectorException {
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

	protected abstract void retrieveResult(CallableStatement cStatement) throws SQLException, DatabaseConnectorException;
	protected abstract void prepareOutput(CallableStatement cStatement) throws SQLException, DatabaseConnectorException;
	protected abstract void prepareInput(CallableStatement cStatement) throws SQLException, DatabaseConnectorException;

	abstract Object getValue();
	abstract void setValue(Object value) throws DatabaseConnectorException;

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
		} else if (value instanceof Struct) {
			objectType = ParameterObject.getType();
		} else if (value instanceof Array) {
			objectType = ParameterList.getType();
		} else {
			throw new DatabaseConnectorException(String.format("Unable to infer data type from value '%s'.", value == null ? "NULL" : value.toString()));
		}
		
		return createParameterFromValue(context, mode, index, value, objectType);
	}

	public static SqlParameter createParameterFromValue(IContext context, ParameterMode mode, int index, Object value, int typeHint) throws DatabaseConnectorException {
		final String objectType;
		switch (typeHint) {
		case java.sql.Types.INTEGER:
			objectType = ParameterLong.getType();
			break;
		case java.sql.Types.DECIMAL:
		case java.sql.Types.NUMERIC:
			objectType = ParameterDecimal.getType();
			break;
		case java.sql.Types.VARCHAR:
			objectType = ParameterString.getType();
			break;
		case java.sql.Types.DATE:
			objectType = ParameterDatetime.getType();
			break;
		case java.sql.Types.ARRAY:
			objectType = ParameterList.getType();
			break;
		case java.sql.Types.STRUCT:
			objectType = ParameterObject.getType();
			break;
		default:
			// Let's try to guess, then?
			return createParameterFromValue(context, mode, index, value);
		}

		return createParameterFromValue(context, mode, index, value, objectType);
	}

	public static SqlParameter createParameterFromValue(IContext context, ParameterMode mode, int index, Object value, String objectType) throws DatabaseConnectorException {
		IMendixObject newObject = Core.instantiate(context, objectType);
		newObject.setValue(context, Parameter.MemberNames.ParameterMode.toString(), mode.toString());
		newObject.setValue(context, Parameter.MemberNames.Position.toString(), index);
		SqlParameter newValue = SqlParameter.initialize(context, newObject);
		newValue.setValue(value);
		return newValue;
	}

}

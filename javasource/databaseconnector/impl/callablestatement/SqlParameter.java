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

/**
 * Base class to wrap parameters for CallableStatements.
 * 
 * Every type of parameter for CallableStatement must have a class extending
 * from this class to prepare the statement before calling and to retrieve its
 * results afterwards.
 */
public abstract class SqlParameter implements Comparable<SqlParameter> {
	protected Parameter parameterObject;

	protected SqlParameter(final IContext context, final IMendixObject mendixParameterObject) {
		this.parameterObject = Parameter.initialize(context, mendixParameterObject);

		if (!this.isNameDefined()
				&& (this.parameterObject.getPosition() == null || this.parameterObject.getPosition() == 0)) {
			throw new IllegalArgumentException("Parameter was initialized without name or position.");
		}
	}

	/**
	 * Method to retrieve the result from a CallableStatement based on parameter
	 * type.
	 * 
	 * For consistency, it should pass the retrieved value to
	 * {@link #setValue(Object)} to set the inner object's value.
	 */
	protected abstract void retrieveResult(CallableStatement cStatement)
			throws SQLException, DatabaseConnectorException;

	/**
	 * Method to register the output value from an executed CallableStatement based
	 * on parameter type.
	 */
	protected abstract void prepareOutput(CallableStatement cStatement) throws SQLException, DatabaseConnectorException;

	/**
	 * Method to set the input value in a CallableStatement based on parameter type.
	 */
	protected abstract void prepareInput(CallableStatement cStatement) throws SQLException, DatabaseConnectorException;

	/**
	 * Should return the value of this parameter.
	 */
	abstract Object getValue();
	
	/**
	 * Should attempt to set this parameter's value to <code>value</code>.
	 */
	abstract void setValue(Object value) throws DatabaseConnectorException;

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
			throw new IllegalArgumentException("Unrecognized parameter type" + this.getParameterMode().toString());
		}
	}

	public Integer getPosition() {
		return this.parameterObject.getPosition();
	}

	public String getName() {
		return this.parameterObject.getName();
	}

	public ParameterMode getParameterMode() {
		return this.parameterObject.getParameterMode();
	}

	protected boolean isNameDefined() {
		return this.getName() != null && !this.getName().isBlank();
	}

	protected String getNameOrPosition() {
		if (this.isNameDefined()) {
			return this.getName();
		} else {
			return this.getPosition().toString();
		}
	}

	// Needed for sorting by position
	@Override
	public int compareTo(SqlParameter other) {
		return this.getPosition() - other.getPosition();
	}

	// Static methods to construct instances of SqlParameter from NPEs or values

	/**
	 * Creates a SqlParameter based on the mendixObject parameter type.
	 * 
	 * @param context      IContext to be used when getting and setting values from
	 *                     the mendixObject.
	 * @param mendixObject Instance of NPE deriving from
	 *                     DatabaseConnector.Parameter.
	 * @return Instance of SqlParameter encapsulating the parameter.
	 */
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
					.retrieveByPath(context, mendixObject,
							ParameterObject.MemberNames.ParameterObject_Parameter.toString())
					.stream().map(p -> initialize(context, p)).sorted().collect(Collectors.toList());
			ret = new SqlParameterObject(context, mendixObject, fields);
			break;
		case ParameterList.entityName:
			List<SqlParameter> elements = Core
					.retrieveByPath(context, mendixObject, ParameterList.MemberNames.ParameterList_Parameter.toString())
					.stream().map(p -> initialize(context, p)).sorted().collect(Collectors.toList());
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

	/**
	 * Method to create a {@link SqlParameter} instance based on a value. Will guess
	 * the type based on the value and delegate object creation to
	 * {@link #createParameterFromValue(IContext, ParameterMode, int, Object, String)}.
	 * 
	 * Throws an exception if unable to guess.
	 * 
	 * @param context  IContext to be used when creating the value NPE.
	 * @param mode     ParameterMode to be used in the resulting SqlParameter.
	 * @param position Parameter position.
	 * @param value    Object containing the value to store.
	 * @return Created object.
	 */
	public static SqlParameter createParameterFromValue(IContext context, ParameterMode mode, int position,
			Object value) throws DatabaseConnectorException {
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
			throw new DatabaseConnectorException(String.format("Unable to infer data type from value '%s'.",
					value == null ? "NULL" : value.toString()));
		}

		return createParameterFromValue(context, mode, position, value, objectType);
	}

	/**
	 * Method to create a {@link SqlParameter} instance based on a type hint. If the
	 * type hint corresponds to a known SQL type which we can convert to a
	 * SqlParameter,
	 * {@link #createParameterFromValue(IContext, ParameterMode, int, Object, String)}
	 * is called. If the type hint is unknown, we attempt to guess the type based on
	 * the value by calling
	 * {{@link #createParameterFromValue(IContext, ParameterMode, int, Object)}.
	 * 
	 * @param context  IContext to be used when creating the value NPE.
	 * @param mode     ParameterMode to be used in the resulting SqlParameter.
	 * @param position Parameter position.
	 * @param value    Object containing the value to store.
	 * @param typeHint JDBC's SQL Type value.
	 * @return Created object.
	 */
	public static SqlParameter createParameterFromValue(IContext context, ParameterMode mode, int position,
			Object value, int typeHint) throws DatabaseConnectorException {
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
			return createParameterFromValue(context, mode, position, value);
		}

		return createParameterFromValue(context, mode, position, value, objectType);
	}

	/**
	 * Creates a Mendix NPE and encapsulates it in the appropriate SqlParameter
	 * object.
	 * 
	 * @param context    IContext to use when creating the NPE.
	 * @param mode       ParameterMode for the Parameter object.
	 * @param position   Position for the Parameter object.
	 * @param value      Value to be used in the Parameter object's specialization.
	 * @param objectType Full name of a specialization for
	 *                   DatabaseConnector.Parameter.
	 * @return A SqlParameter containing a Parameter with <code>value</code>
	 */
	public static SqlParameter createParameterFromValue(IContext context, ParameterMode mode, int position,
			Object value, String objectType) throws DatabaseConnectorException {
		IMendixObject newObject = Core.instantiate(context, objectType);
		newObject.setValue(context, Parameter.MemberNames.ParameterMode.toString(), mode.toString());
		newObject.setValue(context, Parameter.MemberNames.Position.toString(), position);
		SqlParameter newValue = SqlParameter.initialize(context, newObject);
		newValue.setValue(value);
		return newValue;
	}
}

package databaseconnector.impl.callablestatement;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.proxies.ParameterList;
import databaseconnector.proxies.ParameterMode;

/**
 * Representation of an array.
 * 
 * If used as input, we expect all the list members to be in the inner's NPE
 * <code>ParameterList_Parameter</code> association. All list members must have
 * a unique position, indicating its place in the list.
 * 
 * For INPUT or INOUT parameter mode, the list can only be used by position.
 */
public class SqlParameterList extends SqlParameter {
	private final static int SQL_TYPE = java.sql.Types.ARRAY;
	private List<SqlParameter> elements;

	/**
	 * For Oracle we see if the driver is loaded and if call its special Array method instead of the
	 * usual JDBC standard one. See {@link #createArray(Connection)} for the uses of this static members.
	 */
	private final static Class<?> oracleClass;
	private final static Method createOracleArray;
	static {
		Class<?> clazz;
		Method method;
		try {
			clazz = Class.forName("oracle.jdbc.OracleConnection");
			method = clazz.getMethod("createOracleArray", String.class, Object.class);
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
			clazz = null;
			method = null;
		}
		oracleClass = clazz;
		createOracleArray = method;
	}

	public SqlParameterList(final IContext context, IMendixObject mendixObject, List<SqlParameter> elements) {
		super(context, mendixObject);
		this.elements = elements;

		if (!this.getParameterMode().equals(ParameterMode.OUTPUT)) {
			if (this.getPosition() == null || this.getPosition() == 0) {
				throw new IllegalArgumentException("List parameter cannot be used as INPUT or INOUT without a position.");
			}

			Set<Integer> positions = new HashSet<Integer>();
			for (SqlParameter elem : elements) {
				Integer elementPosition = elem.parameterObject.getPosition();
				if (elementPosition == null || elementPosition == 0) {
					throw new IllegalArgumentException("Missing position information for element in list.");
				}
				
				if (positions.contains(elementPosition)) {
					throw new IllegalArgumentException(String.format("Duplicate element at position %d for list parameter.", elem.parameterObject.getPosition()));
				}
				positions.add(elementPosition);
			}
		}
	}

	@Override
	protected void prepareInput(CallableStatement cStatement) throws SQLException, DatabaseConnectorException {
		Array inputArray = createArray(cStatement.getConnection());
		
		if (inputArray == null) {
			throw new IllegalArgumentException("Argument was not an array or it was not possible to convert it to an array.");
		} else {
			// We are only able to set arrays as input by position.
			cStatement.setArray(this.getPosition(), inputArray);
		}
	}

	private Array createArray(Connection connection) throws SQLException, DatabaseConnectorException {
		String sqlTypeName = ((ParameterList) this.parameterObject).getSQLTypeName();
		Object[] attrVals = this.elements.stream().map(SqlParameter::getValue).toArray();

		if (oracleClass != null && connection.isWrapperFor(oracleClass)) {
			try {
				return (Array) createOracleArray.invoke(connection.unwrap(oracleClass), sqlTypeName, attrVals);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new DatabaseConnectorException("Unable to create array structure for input parameter with this Oracle Connection.", e);
			}
		}
		return connection.createArrayOf(sqlTypeName, attrVals);
	}

	@Override
	protected void prepareOutput(CallableStatement cStatement) throws SQLException {
		String sqlTypeName = ((ParameterList) this.parameterObject).getSQLTypeName();
		if (this.isNameDefined()) {
			cStatement.registerOutParameter(this.getName(), SQL_TYPE, sqlTypeName);
		} else {
			cStatement.registerOutParameter(this.getPosition(), SQL_TYPE, sqlTypeName);
		}
	}

	@Override
	protected void retrieveResult(CallableStatement cStatement) throws SQLException, DatabaseConnectorException {
		Array objStruct = null;

		try {
			if (this.isNameDefined()) {
				objStruct = cStatement.getArray(this.getName());
			} else {
				objStruct = cStatement.getArray(this.getPosition());
			}
			this.setValue(objStruct);
		} finally {
			if (objStruct != null) objStruct.free();
		}
	}

	@Override
	List<SqlParameter> getValue() {
		return this.elements;
	}

	@Override
	void setValue(Object value) throws DatabaseConnectorException {
		try {
			if (value != null) {
				Object[] valueArray = (Object[]) ((Array) value).getArray();

				IContext context = this.parameterObject.getContext();
				this.elements.clear();

				int index = 1;
				for (Object val : valueArray) {
					this.elements.add(SqlParameter.createParameterFromValue(context, this.getParameterMode(), index, val));
					index++;
				}

				((ParameterList) this.parameterObject).setParameterList_Parameter(this.elements.stream().map(p -> p.parameterObject).collect(Collectors.toList()));
			}
		} catch (Exception e) {
			throw new DatabaseConnectorException("Unable to set values of array.", e);
		}
	}
}

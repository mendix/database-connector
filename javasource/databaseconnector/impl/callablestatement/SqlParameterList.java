package databaseconnector.impl.callablestatement;

import java.lang.reflect.InvocationTargetException;
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

public class SqlParameterList extends SqlParameter<List<SqlParameter<?>>> {
	private final static int SQL_TYPE = java.sql.Types.ARRAY;
	private List<SqlParameter<?>> elements;

	public SqlParameterList(final IContext context, IMendixObject mendixObject, List<SqlParameter<?>> elements) {
		super(context, mendixObject);
		this.elements = elements;

		if (this.getPosition() == null) {
			throw new IllegalArgumentException("List parameter used as an input was initialized without a position.");
		}
		if (!this.getParameterMode().equals(ParameterMode.OUTPUT)) {
			Set<Integer> positions = new HashSet<Integer>();
			for (SqlParameter<?> elem : elements) {
				if (elem.parameterObject.getPosition() == null) {
					throw new IllegalArgumentException("Missing position information for element in list.");
				}
				
				if (positions.contains(elem.parameterObject.getPosition())) {
					throw new IllegalArgumentException(String.format("Duplicate element at position %d for list parameter.", elem.parameterObject.getPosition()));
				}
				positions.add(elem.parameterObject.getPosition());
			}
		}
	}

	@Override
	protected void prepareInput(CallableStatement cStatement) throws SQLException, DatabaseConnectorException {
		Array inputArray = createArray(cStatement.getConnection());
		
		if (inputArray == null) {
			throw new IllegalArgumentException("Argument was not an array or not possible to convert to an array.");
		} else {
			cStatement.setArray(this.getPosition(), inputArray);
		}
	}

	private Array createArray(Connection connection) throws SQLException, DatabaseConnectorException {
		String sqlTypeName = ((ParameterList) this.parameterObject).getSQLTypeName();
		Object[] attrVals = this.elements.stream().map(SqlParameter::getValue).toArray();
		
		// Oracle uses a different, but almost compatible call type; We use reflection to make it possible to compile this module without having Oracle's drivers present
		try {
			Class<?> oracleClass = Class.forName("oracle.jdbc.OracleConnection");
			if (connection.isWrapperFor(oracleClass)) {
				return (Array) oracleClass.getMethod("createOracleArray",  String.class, Object.class).invoke(connection.unwrap(oracleClass), sqlTypeName, attrVals);
			}
		} catch (ClassNotFoundException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException | SQLException e) {
			// Fallback as usual
		}
		return connection.createArrayOf(sqlTypeName, attrVals);
	}

	@Override
	protected void prepareOutput(CallableStatement cStatement) throws SQLException {
		String sqlTypeName = ((ParameterList) this.parameterObject).getSQLTypeName();
		cStatement.registerOutParameter(this.getPosition(), SQL_TYPE, sqlTypeName);
	}

	@Override
	protected void getValueOutput(CallableStatement cStatement) throws SQLException, DatabaseConnectorException {
		Array objStruct = null;
		try {
			objStruct = retrieveResultArray(cStatement);
			Object[] values = (Object[]) objStruct.getArray();
			IContext context = this.parameterObject.getContext();

			int index = 0;
			for (Object value : values) {
				this.elements.add(SqlParameter.createParameterFromValue(context, this.getParameterMode(), index, value));
				index++;
			}

			((ParameterList) this.parameterObject).setParameterList_Parameter(this.elements.stream().map(p -> p.parameterObject).collect(Collectors.toList()));
		} finally {
			if (objStruct != null) objStruct.free();
		}
	}


	private Array retrieveResultArray(CallableStatement cStatement) throws SQLException {
		String name = this.getName();

		if (name == null || name.isBlank()) {
			return cStatement.getArray(this.getPosition());
		} else {
			return cStatement.getArray(name);
		}
	}

	@Override
	List<SqlParameter<?>> getValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	void setValue(Object value) throws DatabaseConnectorException {
		// TODO Auto-generated method stub
		
			throw new DatabaseConnectorException("Unable to set values of array.", e);
	}
}

package databaseconnector.impl.callablestatement;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.proxies.ParameterList;
import databaseconnector.proxies.ParameterMode;

public class SqlParameterList extends SqlParameter {
	private final static int SQL_TYPE = java.sql.Types.ARRAY;
	private List<SqlParameter> elements;

	public SqlParameterList(final IContext context, IMendixObject mendixObject, List<SqlParameter> elements) {
		super(context, mendixObject);
		this.elements = elements;

		if (this.getParameterMode().equals(ParameterMode.OUTPUT)) {
			if (!this.elements.isEmpty()) {
				throw new IllegalArgumentException("List parameters should have no elements when in OUTPUT mode.");
			}
		} else {
			if (this.getPosition() == null) {
				throw new IllegalArgumentException("List parameter used as an input was initialized without a position.");
			}
			Set<Integer> positions = new HashSet<Integer>();
			for (SqlParameter elem : elements) {
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
	protected void prepareInput(CallableStatement cStatement) throws SQLException {
		Array inputArray = createArray(cStatement.getConnection());
		
		if (inputArray == null) {
			throw new IllegalArgumentException("Argument was not an array or not possible to convert to an array.");
		} else {
			cStatement.setArray(this.getPosition(), inputArray);
		}
	}

	private Array createArray(Connection connection) throws SQLException {
		String sqlTypeName = ((ParameterList) this.parameterObject).getSQLTypeName();
		Object[] attrVals = this.elements.stream().map(SqlParameter::getMxObjectValue).toArray();
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
				SqlParameter valueSqlParameter = SqlParameter.createParameterFromValue(context, this.getParameterMode(), index, value);
				valueSqlParameter.parameterObject.setMemberOfList((ParameterList) this.parameterObject);
				this.elements.add(valueSqlParameter);
				index++;
			}
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
	Object getMxObjectValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	void setMxObjectValue(Object value) {
		// TODO Auto-generated method stub
		
	}
}

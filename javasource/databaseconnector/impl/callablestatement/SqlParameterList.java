package databaseconnector.impl.callablestatement;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterList;
import databaseconnector.proxies.ParameterMode;

public class SqlParameterList implements SqlParameter {
	private final static int SQL_TYPE = java.sql.Types.ARRAY;
	
	private final ParameterList mxObject;
	private List<SqlParameterPrimitiveValue<?>> elements;

	public SqlParameterList(final IContext context, IMendixObject mendixObject, List<SqlParameterPrimitiveValue<?>> elements) {
		this.mxObject = ParameterList.initialize(context, mendixObject);
		this.elements = elements;

		if (this.mxObject.getPosition() == null) {
			throw new IllegalArgumentException("List parameter was initialized without a position.");
		}

		if (this.mxObject.getParameterMode().equals(ParameterMode.OUTPUT)) {
			if (this.elements.size() != 1) {
				throw new IllegalArgumentException("List parameters should have a single element in OUTPUT mode.");
			}
		} else {
			Set<Integer> positions = new HashSet<Integer>();
			for (SqlParameterPrimitiveValue<?> elem : elements) {
				if (elem.mxObject.getPosition() == null) {
					throw new IllegalArgumentException("Missing position information for element in list.");
				}
				
				if (positions.contains(elem.mxObject.getPosition())) {
					throw new IllegalArgumentException(String.format("Duplicate element at position %d for list parameter.", elem.mxObject.getPosition()));
				}
				positions.add(elem.mxObject.getPosition());
			}
		}
	}

	@Override
	public void prepareCall(CallableStatement cStatement) throws SQLException {
		int index = mxObject.getPosition();

		switch (mxObject.getParameterMode()) {
		case INPUT:
			prepareInput(cStatement, index);
			break;
		case OUTPUT:
			prepareOutput(cStatement, index);
			break;
		case INOUT:
			prepareInput(cStatement, index);
			prepareOutput(cStatement, index);
			break;
		default:
			throw new IllegalArgumentException(
					"Unrecognized parameter type" + mxObject.getParameterMode().toString());
		}
	}

	private void prepareInput(CallableStatement cStatement, int index) throws SQLException {
		Array inputArray = createArray(cStatement.getConnection());
		
		if (inputArray == null) {
			throw new IllegalArgumentException("Argument was not an array or not possible to convert to an array.");
		} else {
			cStatement.setArray(index, inputArray);
		}
	}

	private Array createArray(Connection connection) throws SQLException {
		String SQLTypeName = this.mxObject.getSQLTypeName();
		Object[] attrVals = this.elements.stream().map(SqlParameterPrimitiveValue::getMxObjectValue).toArray();
		return connection.createArrayOf(SQLTypeName, attrVals);
	}

	private void prepareOutput(CallableStatement cStatement, int index) throws SQLException {
		String SQLTypeName = this.mxObject.getSQLTypeName();
		cStatement.registerOutParameter(index, SQL_TYPE, SQLTypeName);
	}

	@Override
	public void retrieveResult(CallableStatement cStatement) throws SQLException {
		SqlParameterPrimitiveValue<?> template = elements.get(0);
		if (mxObject.getParameterMode().equals(ParameterMode.OUTPUT) || mxObject.getParameterMode().equals(ParameterMode.INOUT)) {
			Array objStruct = retrieveResultStruct(cStatement);
			Object[] values = (Object[]) objStruct.getArray();
			
			int index = 0;
			for (Object value : values) {
				this.elements.add(newValueFromTemplate(template, index, value));
				index++;
			}
		}
	}

	private SqlParameterPrimitiveValue<?> newValueFromTemplate(SqlParameterPrimitiveValue<?> template, int index, Object value) {
		final IContext context = this.mxObject.getContext();
		IMendixObject newObject = Core.instantiate(context, template.mxObject.getMendixObject().getType());
		newObject.setValue(context, Parameter.MemberNames.ParameterMode.toString(), this.mxObject.getParameterMode().toString());
		newObject.setValue(context, Parameter.MemberNames.MemberOfList.toString(), this.mxObject.getMendixObject().getId());
		newObject.setValue(context, Parameter.MemberNames.Position.toString(), index);
		SqlParameterPrimitiveValue<?> newValue = (SqlParameterPrimitiveValue<?>) SqlParameter.initialize(context, newObject);
		newValue.setMxObjectValue(value);
		return newValue;
	}

	private Array retrieveResultStruct(CallableStatement cStatement) throws SQLException {
		String name = mxObject.getName();
		
		if (name == null || name.isBlank()) {
			return cStatement.getArray(this.mxObject.getPosition());
		} else {
			return cStatement.getArray(name);
		}
	}
}

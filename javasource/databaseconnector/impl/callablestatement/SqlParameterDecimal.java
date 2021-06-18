package databaseconnector.impl.callablestatement;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public class SqlParameterDecimal extends SqlParameterPrimitiveValue {
	public SqlParameterDecimal(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject, java.sql.Types.DECIMAL);
	}

	@Override
	protected Object getStatementValue(CallableStatement cStatement, String name) throws SQLException {
		return cStatement.getBigDecimal(name);
	}

	@Override
	protected Object getStatementValue(CallableStatement cStatement, int position) throws SQLException {
		return cStatement.getBigDecimal(position);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, String name, Object value) throws SQLException {
		cStatement.setBigDecimal(name, (BigDecimal) value);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, int position, Object value) throws SQLException {
		cStatement.setBigDecimal(position, (BigDecimal) value);
	}
}

package databaseconnector.impl.callablestatement;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public class SqlParameterDecimal extends SqlParameterPrimitiveValue<BigDecimal> {
	public SqlParameterDecimal(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject, java.sql.Types.DECIMAL);
	}

	@Override
	protected BigDecimal getStatementValue(CallableStatement cStatement, String name) throws SQLException {
		return cStatement.getBigDecimal(name);
	}

	@Override
	protected BigDecimal getStatementValue(CallableStatement cStatement, int position) throws SQLException {
		return cStatement.getBigDecimal(position);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, String name, BigDecimal value) throws SQLException {
		cStatement.setBigDecimal(name, value);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, int position, BigDecimal value) throws SQLException {
		cStatement.setBigDecimal(position, value);
	}
}

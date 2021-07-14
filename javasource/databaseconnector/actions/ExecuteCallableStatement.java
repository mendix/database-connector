// This file was generated by Mendix Studio Pro.
//
// WARNING: Only the following code will be retained when actions are regenerated:
// - the import list
// - the code between BEGIN USER CODE and END USER CODE
// - the code between BEGIN EXTRA CODE and END EXTRA CODE
// Other code you write will be lost the next time you deploy the project.
// Special characters, e.g., é, ö, à, etc. are supported in comments.

package databaseconnector.actions;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.webui.CustomJavaAction;
import databaseconnector.impl.JdbcConnector;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public class ExecuteCallableStatement extends CustomJavaAction<java.lang.Void>
{
	private java.lang.String jdbcUrl;
	private java.lang.String userName;
	private java.lang.String password;
	private IMendixObject __statement;
	private databaseconnector.proxies.Statement statement;

	public ExecuteCallableStatement(IContext context, java.lang.String jdbcUrl, java.lang.String userName, java.lang.String password, IMendixObject statement)
	{
		super(context);
		this.jdbcUrl = jdbcUrl;
		this.userName = userName;
		this.password = password;
		this.__statement = statement;
	}

	@java.lang.Override
	public java.lang.Void executeAction() throws Exception
	{
		this.statement = __statement == null ? null : databaseconnector.proxies.Statement.initialize(getContext(), __statement);

		// BEGIN USER CODE
		if (this.statement == null) {
			throw new IllegalArgumentException("Execute callable statement was called with an empty value.");
		}
		connector.executeCallableStatement(this.jdbcUrl, this.userName, this.password, this.statement);
		return null;
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 */
	@java.lang.Override
	public java.lang.String toString()
	{
		return "ExecuteCallableStatement";
	}

	// BEGIN EXTRA CODE
	private final ILogNode logNode = Core.getLogger(this.getClass().getName());

	private final JdbcConnector connector = new JdbcConnector(logNode);
	// END EXTRA CODE
}

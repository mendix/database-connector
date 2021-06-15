package databaseconnectortest.test.callablestatement;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.core.IContext;

import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterDatetime;
import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterString;
import databaseconnector.proxies.Statement;

public class StatementBuilder {
		
	private IContext context;
	private Statement statement;
	
	private List<Parameter> parameters = new LinkedList<Parameter>();
	
	public StatementBuilder(IContext context, Statement statement) {
		this.context = context;
		this.statement = statement;
	}
	
	public StatementBuilder(IContext context) {
		this.context = context;
		this.statement = Statement.initialize(context, Core.instantiate(context, Statement.entityName));
	}
	
	public Statement getStatement() {
		return this.statement;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Parameter> T getParameter(int i) {
		return (T)parameters.get(i);
	}

	public StatementBuilder withContentFromFile(String fileName) throws IOException {
		Path fullPath = Core.getConfiguration().getResourcesPath().toPath().resolve(fileName);
		return this.withContent(Files.readString(fullPath));
	}

	public StatementBuilder withContent(String content) {
		statement.setContent(content);
		return this;
	}

	public <T> StatementBuilder withInputParameter(Integer position, String name, T value, Class<?> parameterClass) throws Exception {
		return this.withParameter(position, name, value, ParameterMode.INPUT, parameterClass);
	}
	
	public <T> StatementBuilder withOutputParameter(Integer position, String name, Class<?> parameterClass) throws Exception {
		return this.withParameter(position, name, null, ParameterMode.OUTPUT, parameterClass);
	}
	
	public <T> StatementBuilder withInOutParameter(Integer position, String name, T value, Class<?> parameterClass) throws Exception {
		return this.withParameter(position, name, value, ParameterMode.INOUT, parameterClass);
	}
	
	private <T> StatementBuilder withParameter(Integer position, String name, T value, ParameterMode parameterMode, Class<?> parameterClass) throws Exception {
		
		if (parameterClass == ParameterDatetime.class) {
			ParameterDatetime parameter = ParameterDatetime.initialize(context, Core.instantiate(context, ParameterDatetime.entityName));
			parameter = initParameter(position, name, parameterMode, parameter);
			if (value != null) parameter.setValue((Date) value);
			parameters.add(parameter);
		} else if (parameterClass == ParameterDecimal.class) {
			ParameterDecimal parameter = ParameterDecimal.initialize(context, Core.instantiate(context, ParameterDecimal.entityName));
			parameter = initParameter(position, name, parameterMode, parameter);
			if (value != null) parameter.setValue((BigDecimal) value);
			parameters.add(parameter);
		} else if (parameterClass == ParameterLong.class) {
			ParameterLong parameter = ParameterLong.initialize(context, Core.instantiate(context, ParameterLong.entityName));
			parameter = initParameter(position, name, parameterMode, parameter);
			if (value != null) parameter.setValue((Long) value);
			parameters.add(parameter);
		} else if (parameterClass == ParameterString.class) {
			ParameterString parameter = ParameterString.initialize(context, Core.instantiate(context, ParameterString.entityName));
			parameter = initParameter(position, name, parameterMode, parameter);
			if (value != null) parameter.setValue((String) value);
			parameters.add(parameter);
		}
		
		return this;
	}
	
	private <T extends Parameter> T initParameter(Integer position, String name, ParameterMode parameterMode, T parameter) throws Exception {
		
		parameter.setParameterMode(context, parameterMode);
		if (position != null) parameter.setPosition(position);
		if (name != null) parameter.setName(name);
		parameter.setParameter_Statement(statement);

		return parameter;
	}
}

package databaseconnectortest.test;

import com.mendix.systemwideinterfaces.javaactions.parameters.IStringTemplate;
import com.mendix.systemwideinterfaces.javaactions.parameters.ITemplateParameter;
import com.mendix.systemwideinterfaces.javaactions.parameters.TemplateParameterType;
import databaseconnector.impl.PreparedStatementCreatorImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class PreparedStatementCreatorTest {
	@Rule
	public MockitoRule rule = MockitoJUnit.rule();

	@Mock
	private Connection connection;
	@Mock
	private PreparedStatement preparedStatement;

	private PreparedStatementCreatorImpl sut;

	@Before
	public void before() throws SQLException {
		sut = new PreparedStatementCreatorImpl();
		when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
	}

	@Test
	public void testReusedParameters() throws SQLException {
		StringTemplateBuilder builder = new StringTemplateBuilder();
		builder.addParameter("p1", TemplateParameterType.STRING);
		builder.addParameter("p2", TemplateParameterType.STRING);
		builder.setText("{1} {2} {1} {1} {2}");
		builder.setPlaceholders(List.of(1, 2, 1, 1, 2));

		sut.create(builder.build(), connection);

		verify(preparedStatement).setString(1, "p1");
		verify(preparedStatement).setString(2, "p2");
		verify(preparedStatement).setString(3, "p1");
		verify(preparedStatement).setString(4, "p1");
		verify(preparedStatement).setString(5, "p2");
	}

	@Test
	public void testParameterTypes() throws SQLException {
		StringTemplateBuilder builder = new StringTemplateBuilder();
		Date date = new Date(2019, 5, 1, 14, 12, 11);
		builder.addParameter(date, TemplateParameterType.DATETIME);
		builder.addParameter(5l, TemplateParameterType.INTEGER);
		builder.addParameter(true, TemplateParameterType.BOOLEAN);
		builder.addParameter("Hello", TemplateParameterType.STRING);
		builder.addParameter(new BigDecimal(45.5), TemplateParameterType.DECIMAL);
		builder.addParameter(null, TemplateParameterType.STRING);
		builder.addParameter(null, TemplateParameterType.DATETIME);
		builder.addParameter(null, TemplateParameterType.DECIMAL);
		builder.addParameter(null, TemplateParameterType.BOOLEAN);
		builder.addParameter(null, TemplateParameterType.INTEGER);

		sut.create(builder.build(), connection);

		verify(preparedStatement).setTimestamp(1, new Timestamp(date.getTime()));
		verify(preparedStatement).setLong(2, 5l);
		verify(preparedStatement).setBoolean(3, true);
		verify(preparedStatement).setString(4, "Hello");
		verify(preparedStatement).setBigDecimal(5, new BigDecimal(45.5));
		verify(preparedStatement).setNull(6, Types.VARCHAR);
		verify(preparedStatement).setNull(7, Types.TIMESTAMP);
		verify(preparedStatement).setNull(8, Types.DECIMAL);
		verify(preparedStatement).setNull(9, Types.BOOLEAN);
		verify(preparedStatement).setNull(10, Types.BIGINT);
	}

	@Test
	public void testReplacePlaceholders() throws SQLException {

		StringTemplateBuilder builder = new StringTemplateBuilder();
		builder.setText("Some query");
		builder.setUpdatedText("Updated query");

		sut.create(builder.build(), connection);

		verify(connection).prepareStatement("Updated query");
	}

	@Test
	public void testStringQuery() throws SQLException {
		String query = "Some query";

		sut.create(query, connection);

		verify(connection).prepareStatement(query);
		verifyNoInteractions(preparedStatement);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnknownParameterType() throws SQLException {
		StringTemplateBuilder builder = new StringTemplateBuilder();
		builder.addParameter(null, TemplateParameterType.valueOf("nonexisting value"));

		sut.create(builder.build(), connection);
	}
}

class StringTemplateBuilder {
	ArrayList<ITemplateParameter> templateParameters = new ArrayList<>();
	String template = "";
	String updatedTemplate = "";
	List<Integer> placeholders;

	public StringTemplateBuilder addParameter(Object value, TemplateParameterType type) {

		ITemplateParameter mockParameter = mock(ITemplateParameter.class);
		when(mockParameter.getValue()).thenReturn(value);
		when(mockParameter.getParameterType()).thenReturn(type);

		templateParameters.add(mockParameter);
		return this;
	}

	public StringTemplateBuilder setText(String template) {
		this.template = template;
		return this;
	}

	public StringTemplateBuilder setUpdatedText(String updatedTemplate) {
		this.updatedTemplate = updatedTemplate;
		return this;
	}

	public StringTemplateBuilder setPlaceholders(List<Integer> placeholders) {
		this.placeholders = placeholders;
		return this;
	}

	public IStringTemplate build() {
		IStringTemplate stringTemplate = mock(IStringTemplate.class);

		when(stringTemplate.getParameters()).thenReturn(templateParameters);
		when(stringTemplate.getTemplate()).thenReturn(template);
		when(stringTemplate.replacePlaceholders(any())).thenAnswer(invocation -> {
			Object[] args = invocation.getArguments();
			BiFunction<String, Integer, String> replacer = (BiFunction<String, Integer, String>) args[0];

			if (placeholders == null)
				placeholders = IntStream.range(1, templateParameters.size() + 1).boxed().collect(Collectors.toList());

			placeholders.forEach(idx -> replacer.apply("parameter_" + idx, idx));
			return updatedTemplate;
		});

		return stringTemplate;
	}
}

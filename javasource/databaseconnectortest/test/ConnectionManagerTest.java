package databaseconnectortest.test;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;
import org.mockito.Mockito;

import com.mendix.logging.ILogNode;

import databaseconnector.impl.JdbcConnectionManager;
import databaseconnector.interfaces.ConnectionManager;

public class ConnectionManagerTest {
  private static final String jdbcUrl = "jdbc:hsqldb:mem:testcase;shutdown=true";
  private static final String userName = "TestUserName";
  private static final String password = "TestPassword";

  private ConnectionManager newConnManager(final ILogNode logNode) {
    return new JdbcConnectionManager(logNode);
  }

//  @Before
//  public void setUp() {
//    ILogNode logNode = Mockito.mock(ILogNode.class);
//    ILogManager logManager = Mockito.mock(ILogManager.class);
//    Mockito.when(logManager.getLogNode(Matchers.anyString())).thenReturn(logNode);
//    StaticLoggerBinder.init(logManager);
//  }

  @Test
  public void testGetConnection() throws SQLException {
    ILogNode logger = Mockito.mock(ILogNode.class);
    ConnectionManager manager = newConnManager(logger);

    Connection conn1 = manager.getConnection(jdbcUrl, userName, password);
    Mockito.verify(logger, times(3)).trace(anyString()); // One log message is printed in JdbcConnectionManager.initializeDrivers.
    conn1.close();

    Connection conn2 = manager.getConnection(jdbcUrl, userName, password);
    Mockito.verify(logger, times(4)).trace(anyString());
    conn2.close();

    assertNotEquals(conn1, conn2);
  }

}

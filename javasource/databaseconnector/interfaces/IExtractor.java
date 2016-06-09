package databaseconnector.interfaces;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.Extractor;
import databaseconnector.impl.ResultSetIterator;

public interface IExtractor<T> {
  T extract(ResultSet rs) throws SQLException;

  @SuppressWarnings("unchecked")
  default Stream<T> query(Extractor<IMendixObject> mendixObjectExtractor, ResultSet resultSet, ILogNode logNode) throws SQLException {
    Stream<T> resultStream = (Stream<T>) ResultSetIterator.stream(resultSet, mendixObjectExtractor);
    List<T> resultList = resultStream.collect(Collectors.toList());
    logNode.info(String.format("List: %d", resultList.size()));

    return resultList.stream();
  }
}

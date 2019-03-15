package com.report.handling.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.stereotype.Repository;

@Repository
public class ReportGenerationRepository {

	@Autowired
	JdbcTemplate jdbcTemplate;

	private static final Logger LOGGER = Logger.getLogger(ReportGenerationRepository.class.getName());
	
	
	@Autowired
	DataSource dataSource;
	
	String preparedStatement = "select id,first_name,middle_name,last_name,client_name,org_name," +
            "org_id,manager_name,lead_name,pin,city,country,LongLong" +
            "from test.user " +
            "LIMIT ? OFFSET ?;";
	
	
	String ps = "select * from test.user LIMIT ?";
	
	public ResultSet getResultSet( int batchSize, int batchNumber,Connection connection) 
			throws SQLException, DataAccessException{
		LOGGER.info("Entering ReportGenerationRepository getResultSet");
		ResultSet resultSet = null;

		PreparedStatement ps = connection.prepareStatement(preparedStatement);
		ps.setInt(1, batchSize);
		ps.setInt(2, (batchNumber) * batchSize);
		ps.setFetchSize(batchSize);
		resultSet = ps.executeQuery();
		return resultSet;
	}
	
	public ResultSet getRes(int row) {
		Connection con = null;
		ResultSet resultSet = null;
		try {
			con = dataSource.getConnection();
			PreparedStatement preparedStatement = con.prepareStatement(ps);
			preparedStatement.setInt(1, row);
			resultSet = preparedStatement.executeQuery();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return resultSet;
	}
}

/*jdbcTemplate.execute(preparedStatement,new PreparedStatementCallback<ResultSet>() {

@Override
public ResultSet doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
	ResultSet resultSet;
	ps.setInt(1, batchSize);
	ps.setInt(2, (batchNumber) * batchSize);
	ps.setFetchSize(batchSize);
	resultSet = ps.executeQuery();
	return resultSet;
}
});*/

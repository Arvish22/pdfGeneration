package com.report.handling.utility;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;

import com.report.handling.repository.ReportGenerationRepository;

public class PrintCall implements Callable<String> {
	/*
	 * ReportGenerationRepository reportGenerationRepository = new
	 * ReportGenerationRepository();
	 */
	
	public static volatile long COUNT = 0;
	private static final Logger LOGGER = Logger.getLogger(PrintCall.class.getName());
	private final Connection connection;
	private final int batchSize;
	private final int batchNumber;
	private final String[] headers;
	private final int recordsPerPage;
	 private final String preparedStatementQuery = "SELECT id, first_name, middle_name, last_name, client_name, org_name, org_id, manager_name, lead_name, pin, city, country, longlong, start_date, end_date" + 
	 		"	FROM test.\"user\"" + 
	 		"	LIMIT 10000 OFFSET ?;";
	
	
	    
	public PrintCall(Connection connection,int batchSize, int batchNumber,int recordsPerPage, String[] headers) throws SQLException {
		this.connection = connection;
		connection.setAutoCommit(false);
		this.batchSize = batchSize;
		this.batchNumber = batchNumber;
		this.headers = headers;
		this.recordsPerPage = recordsPerPage;
	}

	@Override
	public String call() {
		// TODO Auto-generated method stub
		ResultSet resultSet = null;
		try {
			PreparedStatement ps = connection.prepareStatement(preparedStatementQuery);
		//	ps.setInt(1, batchSize);
			ps.setInt(1, (batchNumber) * batchSize);
			ps.setFetchSize(batchSize);
			resultSet = ps.executeQuery();
			printRecords(resultSet);
		} catch (DataAccessException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	private void printRecords(ResultSet resultSet) throws SQLException, IOException {
		LOGGER.info("Entering PrintCall printRecords");
        int pageNumber = 0;
        List<Map<String,Object>> rows = new LinkedList<>(); //List of rows
        int rowCounter = 0;
        int columnCount = resultSet.getMetaData().getColumnCount();
        while(resultSet.next()) {
            COUNT++;
            /*List<String> columns = new LinkedList<>();*/  //List Of columns
            Map<String, Object> columns = new LinkedHashMap<>();
            for(int i = 1; i <= columnCount; ++i) {
                Object obj = resultSet.getObject(i);
                if(obj == null) {
                 obj="";
                }
             CharSequence charSequence = obj instanceof CharSequence?(CharSequence)obj:obj.toString();
                columns.put(resultSet.getMetaData().getColumnName(i), charSequence.toString());
            }
            rowCounter++;
            rows.add(columns);
            if(rowCounter == recordsPerPage){
               rowCounter =0;
                PdfWriter pdfWriter = new PdfWriter("Data", "/home/avishwak/anurag/reports");
                pdfWriter.setPageNumber(pageNumber);
                pdfWriter.setRows(rows);
                pdfWriter.start();
                rows = new LinkedList<>(); //List of rows
                pageNumber++;
            }
        }
        if(rowCounter < recordsPerPage){
            PdfWriter pdfWriter = new PdfWriter("Data", "/home/avishwak/anurag/reports");
            pdfWriter.setPageNumber(pageNumber-1);
            pdfWriter.setRows(rows);
            pdfWriter.start();
        }

    }


}

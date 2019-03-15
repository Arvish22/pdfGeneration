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
import java.util.logging.Level;
import java.util.logging.Logger;

public class SQLQuery implements Callable<String> {
    public static volatile long count = 0;
    private static final Logger LOGGER = Logger.getLogger(SQLQuery.class.getName());
    private final Connection connection;
    private final String preparedStatementQuery;
    private final int batchSize;
    private final int batchNumber;
    private final String[] headers;
    private final int recordsPerPage;


    public SQLQuery(Connection connection, String preparedStatementQuery, int batchNumber, int batchSize, int recordsPerPage, String... headers) throws SQLException {
        this.recordsPerPage = recordsPerPage;
        connection.setAutoCommit(false);
        this.connection = connection;
        this.preparedStatementQuery = preparedStatementQuery;
        this.batchSize = batchSize;
        this.batchNumber = batchNumber;
        this.headers = headers;
    }

    @Override
    public String call() throws SQLException {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = this.connection.prepareStatement(this.preparedStatementQuery);
            preparedStatement.setInt(1, batchSize);
            preparedStatement.setInt(2, (batchNumber) * batchSize);
            preparedStatement.setFetchSize(batchSize);
            resultSet = preparedStatement.executeQuery();
            //csv = csvWriter.format(resultSet, headers);
            /* Generate single pdf page */
            //convertToStreamSource(resultSet);
            printRecords(resultSet);
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Connection failure", e);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != resultSet) {
                resultSet.close();
            }
            if (null != preparedStatement) {
                preparedStatement.close();
            }
        }
        return "";
    }

    private void printRecords(ResultSet resultSet) throws SQLException, IOException {
        int pageNumber = 0;
        List<Map<String,Object>> rows = new LinkedList<>(); //List of rows
        int rowCounter = 0;
        int columnCount = resultSet.getMetaData().getColumnCount();
        while(resultSet.next()) {
            count++;
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
                PdfWriter pdfWriter = new PdfWriter("Data", "C:\\tmp");
                pdfWriter.setPageNumber(pageNumber);
                pdfWriter.setRows(rows);
                pdfWriter.start();
                rows = new LinkedList<>(); //List of rows
                pageNumber++;
            }
        }
        if(rowCounter < recordsPerPage){
            PdfWriter pdfWriter = new PdfWriter("Data", "C:\\tmp");
            pdfWriter.setPageNumber(pageNumber-1);
            pdfWriter.setRows(rows);
            pdfWriter.start();
        }

    }

}
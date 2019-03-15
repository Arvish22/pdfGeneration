package com.report.handling.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.pdf.PdfContentByte;
import com.itextpdf.text.pdf.PdfImportedPage;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfWriter;
import com.report.handling.utility.PrintCall;


@Service
public class ReportGenerationService {
		
	@Autowired
	DataSource dataSource;
	
	private static final Logger LOGGER = Logger.getLogger(ReportGenerationService.class.getName());
	private static int FETCH_SIZE = 1000;
	private static int threadCount = 4;
	private static int pageSize;
	private static final int REC_PER_PAGE = 30;
	private static int pageCount;
	private static int recordsCount;
	List<InputStream> inputPdfs = new ArrayList<InputStream>();
	
	public void preparePdf() throws SQLException, IOException, DocumentException{
		LOGGER.info("Entering ReportGenerationService preparePdf");
		   long startTime = System.nanoTime();
	        List<Future<String>> taskList = new ArrayList<>();
	        final Connection connection = dataSource.getConnection();
			recordsCount = getRecordsCount(connection);
	        LOGGER.info("Records Count " + recordsCount);
	        int delta = FETCH_SIZE / REC_PER_PAGE;
	        FETCH_SIZE = delta * REC_PER_PAGE;
	        LOGGER.info("Computed Thread Count" + threadCount);
	        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
			submitTask(executor, taskList,connection);
	        awaitCompletionAll(taskList);
	        executor.shutdown();
	        LOGGER.info("Task count: "+delta);
	        buildList(delta);
	        mergedFile(inputPdfs, new FileOutputStream(new File("/home/avishwak/anurag/reports/report.pdf")));
	        printEndTiming(startTime);
	}
	
	   private void submitTask(ExecutorService executor, List<Future<String>> taskList,Connection connection) throws SQLException {
	        int maxBatchCount = (recordsCount / FETCH_SIZE) + ((recordsCount % FETCH_SIZE) > 0 ? 1 : 0);
	        String[] headers = {"ID",
                    "FirstName",
                    "MiddleName",
                    "LastName",
                    "clientName",
                    "OrgName",
                    "OrgId",
                    "ManagerName",
                    "LeadName",
                    "pin",
                    "city",
                    "country",
                    "longLat"};
	        for (int batchNumber = 0; batchNumber < maxBatchCount; batchNumber++) {
	        	
	        	PrintCall printCall = new PrintCall(connection,FETCH_SIZE, batchNumber, REC_PER_PAGE,headers);
	            final Future<String> task = executor.submit(printCall);
	            taskList.add(task);
	        }
	    }
	   
	   private void awaitCompletionAll(List<Future<String>> taskList) {
	        int threadCounter = 0;
	        int taskSize = taskList.size();
	        while (true) {
	            threadCounter = 0;
	            for (int task = 0; task < taskSize; task++) {
	                if (taskList.get(task).isDone()) {
	                    threadCounter++;
	                }
	            }
	            if (threadCounter == taskSize) {
	                break;
	            }
	            try {
	                LOGGER.info("Sleeping for 100MS");
	                Thread.sleep(100);
	            } catch (InterruptedException e) {
	                LOGGER.log(Level.SEVERE, "Current Thread Was Interrupted", e);
	            }
	        }
	    }
	   
	
	   private int getRecordsCount(Connection con) throws SQLException { 
		/*
		 * ResultSet rs =
		 * con.prepareStatement("select count(*) from test.user").executeQuery(); if
		 * (rs.next()) { return rs.getInt(1); } else {
		 * System.out.println("error: could not get the record counts"); } return 0;
		 */
		   return 1000;
	   }
	 
	   private void printEndTiming(long startTime) {
	        long execEndTime = System.nanoTime();
	        long elapsedTimeExec = execEndTime - startTime;
	        LOGGER.info("Processed time in seconds : "+ (elapsedTimeExec / 1000000)/1000);
	        LOGGER.info("Processed Records : "+ PrintCall.COUNT);
	    }
	   
	   public void buildList(int delta) throws FileNotFoundException {
		   
		   for(int i=0;i<delta;i++) {
			   FileInputStream in = new FileInputStream(new File("/home/avishwak/anurag/reports/Data"+i+".pdf"));
			   inputPdfs.add(in);
		   }
	   }
	   
	   public void mergedFile(List<InputStream> inputPdf, OutputStream outputStream)
				throws IOException, DocumentException {

			Document docs = new Document();

			List<PdfReader> readers = new ArrayList<PdfReader>();

			int totalPage = 0;

			Iterator<InputStream> it = inputPdf.iterator();

			while (it.hasNext()) {
				InputStream pdf = it.next();
				PdfReader reader = new PdfReader(pdf);
				readers.add(reader);
				totalPage = totalPage + reader.getNumberOfPages();
			}

			PdfWriter writer = PdfWriter.getInstance(docs, outputStream);

			docs.open();

			PdfContentByte content = writer.getDirectContent();

			PdfImportedPage imptPage;
			int currentPdfPage = 1;

			Iterator<PdfReader> itRead = readers.iterator();

			while (itRead.hasNext()) {

				PdfReader reader2 = itRead.next();

				while (currentPdfPage <= reader2.getNumberOfPages()) {
					docs.newPage();
					imptPage = writer.getImportedPage(reader2, currentPdfPage);
					content.addTemplate(imptPage, 0, 0);
					currentPdfPage++;
				}
				currentPdfPage = 1;
			}

			outputStream.flush();
			docs.close();
			outputStream.close();

			System.out.println("Done");
		}
}

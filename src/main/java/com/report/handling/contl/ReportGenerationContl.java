package com.report.handling.contl;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.itextpdf.text.DocumentException;
import com.report.handling.repository.ReportGenerationRepository;
import com.report.handling.service.ReportGenerationService;

@RestController
@CrossOrigin(origins = "*")
public class ReportGenerationContl {
	
	@Autowired
	ReportGenerationService reportGenerationService;
	
	@Autowired
	ReportGenerationRepository reportGenerationRepository;
	
	@RequestMapping(value = "/api/test", method=RequestMethod.GET)
	public String  test() throws SQLException{
		ResultSet resultSet = reportGenerationRepository.getRes(5);
		
		String firstName = null;
		while(resultSet.next()) {
			
			firstName = resultSet.getString("first_name");
		}
		
		return firstName;
	}
	
	@RequestMapping(value = "/api/download", method=RequestMethod.GET, produces="application/pdf")
	public ResponseEntity<InputStreamResource>  downloadPdf() throws IOException, DocumentException {
		
		
		
		
		  try { 
			  reportGenerationService.preparePdf();
			  } catch (SQLException e) {
				  //TODO Auto-generated catch block 
				  e.printStackTrace(); 
				  }
		 
		 
			
			ClassPathResource pdf = new ClassPathResource("report.pdf");
			return ResponseEntity
		            .ok()
		            .contentLength(pdf.contentLength())
		            .contentType(
		                    MediaType.parseMediaType("application/octet-stream"))
		            .body(new InputStreamResource(pdf.getInputStream()));
		}
}

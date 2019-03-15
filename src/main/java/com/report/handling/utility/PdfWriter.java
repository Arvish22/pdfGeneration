package com.report.handling.utility;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.xml.DomDriver;
import org.apache.fop.apps.*;

import javax.xml.transform.*;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.util.*;
import java.util.logging.Logger;


public class PdfWriter extends Thread{
    private static final Logger LOGGER = Logger.getLogger(PdfWriter.class.getName());
    private final String prefix;
    private final String folderPath;
    private List<Map<String, Object>> rows;
    private int pageNumber;

    public PdfWriter(String prefix, String folderPath) {
        this.prefix = prefix;
        this.folderPath = folderPath;
    }

    private void print(StreamSource xmlSource, int pageNumber) throws Exception {
        File xsltFile = new File("template.xsl");
        FopFactory fopFactory = FopFactory.newInstance(new File(".").toURI());
        FOUserAgent foUserAgent = fopFactory.newFOUserAgent();
        Fop fop = createfopFactory(fopFactory, foUserAgent, new FileOutputStream(folderPath + File.separator + prefix + pageNumber + ".pdf"));

        Transformer transformer = setUpXSLT(xsltFile);
        Result res = new SAXResult(fop.getDefaultHandler());
        generatePDF(xmlSource, transformer, res);
    }

    private Fop createfopFactory(FopFactory fopFactory, FOUserAgent foUserAgent, OutputStream out) throws FOPException {
        return fopFactory.newFop(MimeConstants.MIME_PDF, foUserAgent, out);
    }

    private Transformer setUpXSLT(File xsltFile) throws TransformerConfigurationException {
        TransformerFactory factory = TransformerFactory.newInstance();
        return factory.newTransformer(new StreamSource(xsltFile));
    }

    private void generatePDF(StreamSource xmlSource, Transformer transformer, Result res) throws TransformerException {
        transformer.transform(xmlSource, res);
    }

    public void print() {
        XStream xStream = new XStream(new DomDriver());
        xStream.registerConverter(new MapEntryConverter());
        xStream.alias("map", java.util.Map.class);
        OutputStream streamSource = new ByteArrayOutputStream();
        List<Columns> columnsList = new LinkedList<>();
        for(int i =0 ; i<rows.size(); i++){
            Map map = rows.get(i);
            Columns columns= new Columns(
            map.get("id"),map.get("first_name"),map.get("middle_name"),map.get("last_name"),map.get("client_name"),map.get("org_name"),map.get("org_id"),map.get("manager_name"),map.get("lead_name"),
            map.get("pin"),map.get("city"),map.get("country"),map.get("longlong"));
            columnsList.add(columns);
        }
        Rows rowsList = new Rows(columnsList);
        xStream.toXML(rowsList, streamSource);
        try {
            print(new StreamSource(new ByteArrayInputStream(((ByteArrayOutputStream) streamSource).toByteArray())), pageNumber);  //Invoke print
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        this.print();
    }

    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
    }

    public void setPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
    }

    public static class MapEntryConverter implements Converter {

        public boolean canConvert(Class clazz) {
            return AbstractMap.class.isAssignableFrom(clazz);
        }

        public void marshal(Object value, HierarchicalStreamWriter writer, MarshallingContext context) {

            AbstractMap map = (AbstractMap) value;
            for (Object obj : map.entrySet()) {
                Map.Entry entry = (Map.Entry) obj;
                writer.startNode(entry.getKey().toString());
                Object val = entry.getValue();
                if ( null != val ) {
                    writer.setValue(val.toString());
                }
                writer.endNode();
            }

        }

        public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {

            Map<String, String> map = new HashMap<String, String>();

            while(reader.hasMoreChildren()) {
                reader.moveDown();
                String key = reader.getNodeName(); // nodeName aka element's name
                String value = reader.getValue();
                map.put(key, value);
                reader.moveUp();
            }

            return map;
        }

    }

}

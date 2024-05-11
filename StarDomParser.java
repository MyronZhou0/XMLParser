import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.sql.*;

import java.util.ArrayList;
import java.util.List;

public class StarDomParser {
    private List<Star> stars = new ArrayList<>();
    private Document dom;
    private int starIdCount = 1; // Start from 1 or another starting point
    private Connection conn;

    public StarDomParser() throws SQLException, ClassNotFoundException {
        init();
    }

    private void init() throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/moviedb?autoReconnect=true&useSSL=false", "mytestuser", "My6$Password");
    }

    public void runExample() {
        try {
            parseXmlFile();
            parseDocument();
            printData();
            insertStars();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) conn.close(); // Ensure connection is closed
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void parseXmlFile() {
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            dom = dBuilder.parse("actors63.xml");
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    private void parseDocument() {
        Element docElement = dom.getDocumentElement();
        NodeList nodeList = docElement.getElementsByTagName("actor");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element element = (Element) nodeList.item(i);
            Star star = parseStar(element);
            stars.add(star);
        }
    }

    private Star parseStar(Element element) {
        String id = "nm" + (9423080 + starIdCount); // Prefix "nm" added to generated ID
        starIdCount++;
        String name = getTextValue(element, "stagename");
        int birthYear = getIntValue(element, "dob");
        return new Star(id, name, birthYear);
    }

    private String getTextValue(Element elem, String tagName) {
        NodeList nl = elem.getElementsByTagName(tagName);
        if (nl != null && nl.getLength() > 0) {
            Element el = (Element) nl.item(0);
            if (el.getFirstChild() != null) {
                return el.getFirstChild().getNodeValue();
            }
        }
        return null;
    }

    private int getIntValue(Element elem, String tagName) {
        try {
            return Integer.parseInt(getTextValue(elem, tagName));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return -1; // Use -1 as default/error value
        }
    }

    private void printData() {
        System.out.println("Total parsed " + stars.size() + " stars including duplicates");
        for (Star star : stars) {
            System.out.println("\t" + star);
        }
    }

    private void insertStars() throws SQLException {
        conn.setAutoCommit(false);
        String callSQL = "{CALL InsertStar(?, ?, ?)}";  // SQL to call the stored procedure
        try (CallableStatement cstmt = conn.prepareCall(callSQL)) {
            int count = 0;
            for (Star star : stars) {
                cstmt.setString(1, star.getId());
                cstmt.setString(2, star.getName());
                cstmt.setInt(3, star.getBirthYear());
                cstmt.addBatch();  // Add to batch
                count++;
                if (count % 1000 == 0 || count == stars.size()) {
                    cstmt.executeBatch();  // Execute batch
                    conn.commit();  // Commit transaction
                }
            }
        }
    }


    public static void main(String[] args) {
        try {
            StarDomParser domParser = new StarDomParser();
            domParser.runExample();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

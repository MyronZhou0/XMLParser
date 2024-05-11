import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.sql.*;

import java.util.ArrayList;
import java.util.List;

public class StarsInMoviesDomParser {
    private List<StarInMovie> stars_in_movies = new ArrayList<>();
    private Document dom;
    private Connection conn;

    public StarsInMoviesDomParser() throws SQLException, ClassNotFoundException {
        init();
    }

    public void init() throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/moviedb?autoReconnect=true&useSSL=false",
                "mytestuser", "My6$Password");
    }

    public void runExample() {
        try {
            parseXmlFile();
            parseDocument();
            printData();
            insertStarsInMovies();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void parseXmlFile() {
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            dom = dBuilder.parse("casts124.xml");
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    private void parseDocument() {
        Element docElement = dom.getDocumentElement();
        NodeList nodeList = docElement.getElementsByTagName("m");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element element = (Element) nodeList.item(i);
            StarInMovie starinmovie = parseStarInMovie(element);
            stars_in_movies.add(starinmovie);
        }
    }

    private StarInMovie parseStarInMovie(Element element) {
        int genreId = 25; // Default genre ID
        String movieId = getTextValue(element, "f");
        String starName = getTextValue(element,"a");

        return new StarInMovie(movieId, starName);
    }

    private String getTextValue(Element elem, String tagName) {
        NodeList nl = elem.getElementsByTagName(tagName);
        if (nl != null && nl.getLength() > 0) {
            Element el = (Element) nl.item(0);
            Node node = el.getFirstChild();
            if (node != null) {
                return node.getNodeValue();
            }
        }
        return null;
    }

//    private int getIntValue(Element elem, String tagName) {
//        try {
//            return Integer.parseInt(getTextValue(elem, tagName));
//        } catch (NumberFormatException e) {
//            e.printStackTrace();
//            return -1;
//        }
//    }

    private void printData() {
        System.out.println("Total parsed " + stars_in_movies.size() + " movies");
        for (StarInMovie starinmovie : stars_in_movies) {
            System.out.println(starinmovie);
        }
    }

    private void insertStarsInMovies() throws SQLException {
        conn.setAutoCommit(false);
        String callSQL = "{CALL InsertStarInMovie(?, ?)}";  // SQL to call the stored procedure
        try (CallableStatement cstmt = conn.prepareCall(callSQL)) {
            int count = 0;
            for (StarInMovie starinmovie : stars_in_movies) {
                cstmt.setString(1, starinmovie.getMovieId());
                cstmt.setString(2, starinmovie.getStarName());
                cstmt.addBatch();  // Add to batch
                count++;
                if (count % 1000 == 0 || count == stars_in_movies.size()) {
                    cstmt.executeBatch();  // Execute batch
                    conn.commit();  // Commit transaction
                }
            }
        } catch (SQLException e) {
            conn.rollback();  // Rollback in case of any error during batch execution
            throw e;
        }
    }

    public static void main(String[] args) {
        try {
            StarsInMoviesDomParser domParser = new StarsInMoviesDomParser();
            domParser.runExample();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

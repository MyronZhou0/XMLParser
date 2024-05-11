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

public class MovieDomParser {
    private List<Movie> movies = new ArrayList<>();
    private Document dom;
    private Connection conn;

    public MovieDomParser() throws SQLException, ClassNotFoundException {
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
            insertMovies();
            insertGenresInMovies();
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
            dom = dBuilder.parse("mains243.xml");
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    private void parseDocument() {
        Element docElement = dom.getDocumentElement();
        NodeList nodeList = docElement.getElementsByTagName("film");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element element = (Element) nodeList.item(i);
            Movie movie = parseMovie(element);
            movies.add(movie);
        }
    }

    private Movie parseMovie(Element element) {
        //genreId for matching to genres_in_movies
        int genreId;
        String id = getTextValue(element, "fid");
        String title = getTextValue(element, "t");
        int year = getIntValue(element, "year");
        String director = getTextValue(element, "dirn");
        String genre = getTextValue(element, "cat");
        if(genre == null)
        {
            genreId = 0;
        }
        else
        {
            switch (genre) {
                case "Susp":
                    genreId = 21;
                    break;
                case "CnR":
                    genreId = 7;
                    break;
                case "Dram":
                    genreId = 9;
                    break;
                case "West":
                    genreId = 23;
                    break;
                case "Myst":
                    genreId = 16;
                    break;
                case "S.F.":
                    genreId = 19;
                    break;
                case "Advt":
                    genreId = 3;
                    break;
                case "Horr":
                    genreId = 13;
                    break;
                case "Comd":
                    genreId = 6;
                    break;
                case "Musc":
                    genreId = 15;
                    break;
                case "Docu":
                    genreId = 8;
                    break;
                case "Porn":
                    genreId = 2;
                    break;
                case "Noir":
                    genreId = 24;
                    break;
                case "BioP":
                    genreId = 26;
                    break;
                default:
                    genreId = 25;
                    break;
            }
        }
        return new Movie(id, title, year, director, genre, genreId);
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

    private int getIntValue(Element elem, String tagName) {
        try {
            return Integer.parseInt(getTextValue(elem, tagName));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return -1;
        }
    }

    private void printData() {
        System.out.println("Total parsed " + movies.size() + " movies");
        for (Movie movie : movies) {
            System.out.println(movie);
        }
    }

    private void insertMovies() throws SQLException {
        conn.setAutoCommit(false);
        String callSQL = "{CALL InsertMovie(?, ?, ?, ?)}";
        try (CallableStatement cstmt = conn.prepareCall(callSQL)) {
            int count = 0;
            for (Movie movie : movies) {
                cstmt.setString(1, movie.getId());
                cstmt.setString(2, movie.getTitle());
                cstmt.setInt(3, movie.getYear());
                cstmt.setString(4, movie.getDirector());
                cstmt.addBatch();
                count++;
                if (count % 1000 == 0 || count == movies.size()) {
                    cstmt.executeBatch();
                    conn.commit();
                }
            }
        }
    }


    private void insertGenresInMovies() throws SQLException {
        conn.setAutoCommit(false);
        String callSQL = "{CALL InsertGenreInMovie(?, ?)}";
        try (CallableStatement cstmt = conn.prepareCall(callSQL)) {
            int count = 0;
            for (Movie movie : movies) {
                cstmt.setInt(1, movie.getGenreId());
                cstmt.setString(2, movie.getId());
                cstmt.addBatch();
                count++;
                if (count % 1000 == 0 || count == movies.size()) {
                    cstmt.executeBatch();
                    conn.commit();
                }
            }
        }
    }


    public static void main(String[] args) {
        try {
            MovieDomParser domParser = new MovieDomParser();
            domParser.runExample();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

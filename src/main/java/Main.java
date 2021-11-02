import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Main {
    static Connection mysql;
    static Connection neo4j;
    static final String url_mysql = "jdbc:mysql://127.0.0.1:3306/instagram";
    static final String user_mysql = "mf";
    static final String pass = "root";
    static final String url_neo4j = "jdbc:neo4j:bolt://localhost:7687";
    static final String user_neo4j = "neo4j";

    public static void main(String[] args) {
        conNeo4j();
        conMySQL();
        clearDB();
        getBenutzer();
        getFollower();
        getVeroeffentlichungen();
        getVeroeffentlichungenRel();
        getKommentare();
        getLikes();
        getBKRel();
        getKVRel();
        getBLRel();
        getLVRel();
        System.out.println("\nDie Datenbank wurde vollständig gelesen und geschrieben");
    }

    public static void conNeo4j() {
        try {
            neo4j = DriverManager.getConnection(url_neo4j, user_neo4j, pass);
            System.out.println("\nDie Verbindung zu Neo4j wurde erfolgreich hergestellt");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void conMySQL() {
        try {
            mysql = DriverManager.getConnection(url_mysql, user_mysql, pass);
            System.out.println("\nDie Verbindung zu MySQL wurde erfolgreich hergestellt");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void clearDB() {
        try {
            Statement stm = neo4j.createStatement();
            String abfrage = "match (a) -[r] -> () delete a, r";
            String abfrage2 = "match (a) delete a";
            stm.executeQuery(abfrage);
            stm.executeQuery(abfrage2);
            System.out.println("\nDie Datenbank wurde erfolgreich zurückgesetzt");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /* --------------------------------------MYSQL-----------------------------------------------------*/

    public static void getBenutzer() {
        try {
            Statement stm = mysql.createStatement();
            String abfrage = "SELECT * FROM benutzer";
            ResultSet rs = stm.executeQuery(abfrage);
            while(rs.next()) {
                addBenutzer(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4));
            }
            System.out.println("\nDie Benutzer wurden aus der Datenbank gelesen");
            System.out.println("\nDie Benutzer wurden der Datenbank hinzugefügt");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void getFollower() {
        try {
            Statement stm = mysql.createStatement();
            String abfrage = "SELECT * FROM follower";
            ResultSet rs = stm.executeQuery(abfrage);
            while(rs.next()) {
                addFollower(rs.getString(2), rs.getString(3));
            }
            System.out.println("\nDie Follower wurden aus der Datenbank gelesen");
            System.out.println("\nDie Follower wurden der Datenbank hinzugefügt");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void getVeroeffentlichungen() {
        try {
            Statement stm = mysql.createStatement();
            String abfrage = "SELECT * FROM Veröffentlichungen";
            ResultSet rs = stm.executeQuery(abfrage);
            while(rs.next()) {
                addVeroeffentlichungen(rs.getString(1), rs.getString(2), rs.getString(3));
            }
            System.out.println("\nDie Veröffentlichungen wurden aus der Datenbank gelesen");
            System.out.println("\nDie Veröffentlichungen wurden der Datenbank hinzugefügt");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void getVeroeffentlichungenRel() {
        try {
            Statement stm = mysql.createStatement();
            String abfrage = "SELECT * FROM Benutzer";
            ResultSet rs = stm.executeQuery(abfrage);
            while(rs.next()) {
                addVeroeffentlichungenRel(rs.getString(1));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void getKommentare() {
        try {
            Statement stm = mysql.createStatement();
            String abfrage = "SELECT * FROM Kommentare";
            ResultSet rs = stm.executeQuery(abfrage);
            while(rs.next()) {
                addKommentare(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5));
            }
            System.out.println("\nDie Kommentare wurden aus der Datenbank gelesen");
            System.out.println("\nDie Kommentare wurden der Datenbank hinzugefügt");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void getBKRel() {
        try {
            Statement stm = mysql.createStatement();
            String abfrage = "SELECT * FROM Benutzer";
            ResultSet rs = stm.executeQuery(abfrage);

            while(rs.next()) {
                addBKRel(rs.getString(1));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void getKVRel() {
        try {
            Statement stm = mysql.createStatement();
            String abfrage = "SELECT * FROM Veröffentlichungen";
            ResultSet rs = stm.executeQuery(abfrage);

            while(rs.next()) {
                addKVRel(rs.getString(1));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void getLikes() {
        try {
            Statement stm = mysql.createStatement();
            String abfrage = "SELECT * FROM Likes";
            ResultSet rs = stm.executeQuery(abfrage);
            while(rs.next()) {
                addLikes(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4));
            }
            System.out.println("\nDie Likes wurden aus der Datenbank gelesen");
            System.out.println("\nDie Likes wurden der Datenbank hinzugefügt");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void getBLRel() {
        try {
            Statement stm = mysql.createStatement();
            String abfrage = "SELECT * FROM Benutzer";
            ResultSet rs = stm.executeQuery(abfrage);

            while(rs.next()) {
                addBLRel(rs.getString(1));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void getLVRel() {
        try {
            Statement stm = mysql.createStatement();
            String abfrage = "SELECT * FROM Veröffentlichungen";
            ResultSet rs = stm.executeQuery(abfrage);

            while(rs.next()) {
                addLVRel(rs.getString(1));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /* --------------------------------------NEO4J-----------------------------------------------------*/

    public static void addBenutzer(String a, String b, String c, String d) {
        try {
            Statement stm = neo4j.createStatement();
            String abfrage = "create (b:Benutzer {nutzername: '"+a+"', vorname:'"+b+"', nachname:'"+c+"', geburtsdatum:'"+d+"'})";
            stm.executeQuery(abfrage);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void addFollower(String a, String b) {
        try {
            Statement stm = neo4j.createStatement();
            String abfrage = "MATCH (b:Benutzer), (b1:Benutzer) WHERE b.nutzername = '"+ a +"' AND b1.nutzername = '" + b + "' CREATE (b)-[r:folgt]->(b1) RETURN r";
            stm.executeQuery(abfrage);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void addVeroeffentlichungen(String a, String b, String c) {
        try {
            Statement stm = neo4j.createStatement();
            String abfrage = "create (v:Veröffentlichung {id: '"+a+"', nutzername:'"+b+"', datum:'"+c+"'})";
            stm.executeQuery(abfrage);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    private static void addVeroeffentlichungenRel(String a) {
        try {
            Statement stm = neo4j.createStatement();
            String abfrage = "MATCH (b:Benutzer), (v:Veröffentlichung) WHERE b.nutzername = '"+ a +"' AND v.nutzername = '" + a + "' CREATE (b)-[r:veröffentlicht]->(v) RETURN r";
            stm.executeQuery(abfrage);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void addKommentare(String a, String b, String c, String d, String e) {
        try {
            Statement stm = neo4j.createStatement();
            String abfrage = "create (k:Kommentare {id: '"+a+"', kommentartext:'"+b+"', nutzername:'"+c+"', veröffentlichungsid:'"+d+"', datum:'"+e+"'})";
            stm.executeQuery(abfrage);

        } catch (SQLException f) {
            System.out.println(f.getMessage());
        }
    }

    private static void addLikes(String a, String b, String c, String d) {
        try {
            Statement stm = neo4j.createStatement();
            String abfrage = "create (l:Likes {id: '"+a+"', nutzername:'"+b+"', veröffentlichungsid:'"+c+"', datum:'"+d+"'})";
            stm.executeQuery(abfrage);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void addBKRel(String a) {
        try {
            Statement stm = neo4j.createStatement();
            String abfrage = "MATCH (b:Benutzer),(k:Kommentare) WHERE b.nutzername = '"+ a +"' AND k.nutzername = '" + a + "' CREATE (b)-[r:macht]->(k) RETURN r";
            stm.executeQuery(abfrage);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void addKVRel(String a) {
        try {
            Statement stm = neo4j.createStatement();
            String abfrage = "MATCH (k:Kommentare), (v:Veröffentlichung) WHERE k.veröffentlichungsid = '" + a + "' AND v.id = '" + a + "' CREATE (k)-[r:zu]->(v) RETURN r";
            stm.executeQuery(abfrage);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void addBLRel(String a) {
        try {
            Statement stm = neo4j.createStatement();
            String abfrage = "MATCH (b:Benutzer),(l:Likes) WHERE b.nutzername = '"+ a +"' AND l.nutzername = '" + a + "' CREATE (b)-[r:tätigt]->(l) RETURN r";
            stm.executeQuery(abfrage);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void addLVRel(String a) {
        try {
            Statement stm = neo4j.createStatement();
            String abfrage = "MATCH (l:Likes), (v:Veröffentlichung) WHERE l.veröffentlichungsid = '" + a + "' AND v.id = '" + a + "' CREATE (l)-[r:zu]->(v) RETURN r";
            stm.executeQuery(abfrage);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

}
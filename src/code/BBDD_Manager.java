package code;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import org.apache.commons.dbcp2.BasicDataSource;

/**
 *
 * @author David
 */
public class BBDD_Manager {

    Connection connect = null;

    public boolean connection() {
        //Realizamos la conexion a la base de datos
        BasicDataSource bdSource = new BasicDataSource();
        bdSource.setUrl("jdbc:mysql://localhost:3306/?serverTimezone=UTC");
        bdSource.setUsername("root");
        bdSource.setPassword("");
        Statement sta;
        boolean connected = false;
        try {
            connect = bdSource.getConnection();
            if (connect != null) {
                if (!checkDatBaseCreated()) {
                    createDataBase();
                }
                sta = connect.createStatement();
                ResultSet r = sta.executeQuery("use videoclub;");
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }

    public void disconnection() {
        //Cerramos la base de datos
        try {
            connect.close();
        } catch (Exception e) {
        }
    }

    public boolean checkDatBaseCreated() {
        String query;
        try {
            Statement sta = connect.createStatement();
            query = "SHOW DATABASES;";

            ResultSet rs = sta.executeQuery(query);
            rs = sta.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();

            int i = 0;
            while (rs.next()) {
                if (rs.getString("database").equals("videoclub")) {
                    return true;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    public void createDataBase() {
        //Ejecutamos todas las funciones
        Statement sta;
        try {
            sta = connect.createStatement();
            String[] query = {
                "DROP DATABASE IF EXISTS videoclub;",
                "CREATE DATABASE if NOT EXISTS videoclub;",
                "use videoclub;",
                "CREATE TABLE IF NOT EXISTS SERIES( id INT (11) NOT NULL auto_increment, nombre VARCHAR (255) NOT NULL, director VARCHAR (255) NOT NULL, plataforma INT (11) NOT NULL, fechasalida DATE NOT NULL, fechafinalizacion DATE, PRIMARY KEY (id));",
                "CREATE TABLE IF NOT EXISTS PELICULAS( id INT (11) NOT NULL auto_increment, nombre VARCHAR (255) NOT NULL, director VARCHAR (255) NOT NULL, plataforma int (11) NOT NULL, fechasalida DATE NOT NULL, PRIMARY KEY (id)) ;",
                "CREATE TABLE IF NOT EXISTS PLATAFORMAS( id INT (11) NOT NULL auto_increment, nombre VARCHAR (255) NOT NULL, precio DECIMAL (11, 2) NOT NULL, fechasalida DATE NOT NULL, PRIMARY KEY (id) );",
                "ALTER TABLE peliculas ADD FOREIGN KEY(plataforma) REFERENCES plataformas(id);",
                "ALTER TABLE series ADD FOREIGN KEY (plataforma) REFERENCES plataformas(id);",
                "INSERT INTO PLATAFORMAS(nombre, precio, fechasalida) VALUES ('Netflix', 10.95, '1997-08-29'), ('Disney+', 7.95, '2019-12-12'), ('Amazon Prime video', 9.95, '2006-09-07');",
                "INSERT INTO  SERIES( nombre, director, plataforma,  fechasalida, fechafinalizacion ) VALUES  ( 'Gambito de Dama', 'Scott Frank',  1, '2020-10-23', NULL ),  ( 'El mandaloriano', 'Jon Favreau',  2, '2019-12-12', NULL ), ( 'Breaking Bad', ' Vince Gilligan', 1, '2008-01-20', '2013-09-29' ), ( 'Rick y Morty', 'Pete Michels', 1, '2013-09-2', NULL ), ( 'Peaky Blinders', 'Steven Knight',  1, '2013-09-12', NULL ), ( 'Hijos de la Anarquia', 'Kurt Sutter', 1, '2008-09-03', '2014-12-09' ), ( 'The Boys', 'Eric Kripke', 3, '2019-07-26',  NULL ),( 'The witcher', 'Lauren Schmidt Hissrich',  1, '2018-7-17', NULL ),( 'WandaVision', 'Jac Schaeffer', 2, '2020-1-22', NULL );",
                "INSERT INTO PELICULAS(nombre, director, plataforma, fechasalida) VALUES  ( 'Django desencadenado', 'Quentin Tarantino', 1, '2012-01-25' ), ( 'Reservoir dogs', 'Quentin Tarantino', 3, '1992-01-23' ),( 'El lobo de Wall Street', '	Martin Scorsese', 1, '2013-02-22' ),( 'Puñales por la espalda', 'Rian Johnson', 3, '2019-08-12' ),( 'Star wars 5 : El imperio contrataca', 'Irvin Kershner', 2, '1980-04-04' ),( 'Star wars 4 : Una nueva esperanza', 'George Lucas', 2, '1977-05-25' );;",};
            for (int i = 0; i < query.length; i++) {
                sta.executeUpdate(query[i]);
            }
            sta.close();
        } catch (Exception e) {
        }
    }

    public String[][] getFilms() {
        String[][] data;
        Statement sta;
        try {
            sta = connect.createStatement();

            ResultSet rs1 = sta.executeQuery("SELECT COUNT(*) as counter FROM peliculas;");
            rs1.next();

            data = new String[rs1.getInt("counter")][4];
            ResultSet rs = sta.executeQuery("SELECT pe.*, pl.nombre as plataformaNombre from peliculas pe inner join plataformas pl on  pl.id= pe.plataforma;");
            int i = 0;
            while (rs.next()) {
                data[i][0] = rs.getString("nombre");
                data[i][1] = rs.getString("director");
                data[i][2] = rs.getString("plataformaNombre");
                data[i][3] = rs.getString("fechasalida");
                i++;
            }

            return data;
        } catch (Exception e) {
            return null;
        }

    }

    public String[][] getSeries() {
        String[][] data;
        Statement sta;
        try {
            sta = connect.createStatement();

            ResultSet rs1 = sta.executeQuery("SELECT COUNT(*) as counter FROM series;");
            rs1.next();

            data = new String[rs1.getInt("counter")][5];
            ResultSet rs = sta.executeQuery("SELECT s.*, p.nombre as plataformaNombre from series s inner join plataformas p on  p.id= s.plataforma;");
            int i = 0;
            while (rs.next()) {
                data[i][0] = rs.getString("nombre");
                data[i][1] = rs.getString("director");
                data[i][2] = rs.getString("plataformaNombre");
                data[i][3] = rs.getString("fechasalida");
                data[i][4] = rs.getString("fechafinalizacion");
                i++;
            }

            return data;
        } catch (Exception e) {
            return null;
        }
    }

    public ArrayList<String> getPlatforms() {
        ArrayList<String> platforms = new ArrayList<String>();
        Statement sta;
        try {

            sta = connect.createStatement();
            ResultSet rs = sta.executeQuery("SELECT nombre from plataformas;");
            while (rs.next()) {
                platforms.add(rs.getString("nombre"));
            }
        } catch (Exception e) {
        }
        return platforms;
    }

    public String[][] getSeriesandMovies(String plataforma) {
        String[][] data;
        Statement sta;
        try {
            sta = connect.createStatement();
            ResultSet r = sta.executeQuery("SELECT COUNT(*) as counter FROM series WHERE plataforma ='" + plataforma + "';");
            r.next();
            int aux = r.getInt("counter");
            ResultSet r_ = sta.executeQuery("SELECT COUNT(*) as counter FROM peliculas WHERE plataforma ='" + plataforma + "';");
            r_.next();
            aux += r_.getInt("counter");

            data = new String[aux][5];
            ResultSet rs = sta.executeQuery("SELECT s.*, p.nombre as plataformaNombre from series s inner join plataformas p on  p.id= s.plataforma WHERE s.plataforma='" + plataforma + "'");
            int i = 0;
            while (rs.next()) {
                data[i][0] = rs.getString("nombre");
                data[i][1] = rs.getString("director");
                data[i][2] = rs.getString("plataformaNombre");
                data[i][3] = rs.getString("fechasalida");
                data[i][4] = rs.getString("fechafinalizacion");
                i++;
            }
            ResultSet rs2 = sta.executeQuery("SELECT pe.*, pl.nombre as plataformaNombre from peliculas pe inner join plataformas pl on  pl.id= pe.plataforma WHERE pe.plataforma='" + plataforma + "'");
            while (rs2.next()) {
                data[i][0] = rs2.getString("nombre");
                data[i][1] = rs2.getString("director");
                data[i][2] = rs2.getString("plataformaNombre");
                data[i][3] = rs2.getString("fechasalida");
                i++;
            }
            return data;
        } catch (Exception e) {
            return null;
        }
    }

    public String[][] getAllSeriesandMovies() {
        String[][] data;
        Statement sta;
        try {
            sta = connect.createStatement();
            ResultSet r = sta.executeQuery("SELECT COUNT(*) as counter FROM series ;");
            r.next();
            int aux = r.getInt("counter");
            ResultSet r_ = sta.executeQuery("SELECT COUNT(*) as counter FROM peliculas;");
            r_.next();
            aux += r_.getInt("counter");

            data = new String[aux][5];
            ResultSet rs = sta.executeQuery("SELECT s.*, p.nombre as plataformaNombre from series s inner join plataformas p on  p.id= s.plataforma;");
            int i = 0;
            while (rs.next()) {
                data[i][0] = rs.getString("nombre");
                data[i][1] = rs.getString("director");
                data[i][2] = rs.getString("plataformaNombre");
                data[i][3] = rs.getString("fechasalida");
                data[i][4] = rs.getString("fechafinalizacion");
                i++;
            }
            ResultSet rs2 = sta.executeQuery("SELECT pe.*, pl.nombre as plataformaNombre from peliculas pe inner join plataformas pl on  pl.id= pe.plataforma ");
            while (rs2.next()) {
                data[i][0] = rs2.getString("nombre");
                data[i][1] = rs2.getString("director");
                data[i][2] = rs2.getString("plataformaNombre");
                data[i][3] = rs2.getString("fechasalida");
                i++;
            }
            return data;
        } catch (Exception e) {
            return null;
        }
    }

    public void deleteSerieOrMovies(String name) {
        //Con este metodo borramos la cancion
        Statement sta;
        try {
            sta = connect.createStatement();
            sta.executeUpdate("Delete from peliculas where nombre='" + name + "';");
            sta.close();
        } catch (Exception e) {
        }
        try {
            sta = connect.createStatement();
            sta.executeUpdate("Delete from series where nombre='" + name + "';");
            sta.close();
        } catch (Exception e) {
        }
    }

    public boolean addMovie(String name, String director, int plataforma, String fecha) {

        Statement sta;
        try {
            sta = connect.createStatement();
            sta.executeUpdate("INSERT INTO PELICULAS(nombre, director, plataforma, fechasalida) VALUES ( '" + name + "','" + director + "' , " + plataforma + " , '" + fecha + "' );");
            sta.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean addSeries(String name, String director, int plataforma, String fechaSalida, String fechaFinalizacion) {
        if (fechaFinalizacion.equals("")) {
            Statement sta;
            try {
                sta = connect.createStatement();
                sta.executeUpdate("INSERT INTO  SERIES( nombre, director, plataforma,  fechasalida, fechafinalizacion ) VALUES ( '" + name + "','" + director + "' , " + plataforma + " , '" + fechaSalida + "',NULL);");
                sta.close();
                return true;
            } catch (Exception e) {
                return false;
            }
        } else {
            Statement sta;
            try {
                sta = connect.createStatement();
                sta.executeUpdate("INSERT INTO  SERIES( nombre, director, plataforma,  fechasalida, fechafinalizacion ) VALUES ( '" + name + "','" + director + "' , " + plataforma + " , '" + fechaSalida + "','" + fechaFinalizacion + "' );");
                sta.close();
                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }

    public boolean addPlatform(String name, Double precio, String date) {
        Statement sta;
        try {
            sta = connect.createStatement();
            sta.executeUpdate("INSERT INTO PLATAFORMAS(nombre, precio, fechasalida) VALUES ('" + name + "'," + precio + " , '" + date + "');");
            sta.close();
            return true;
        } catch (Exception e) {
            return false;
        }

    }

    public void updateSerieOrMovie(String old_name, String new_name, String new_director, int new_platform) {
        try {
            Statement sta;
            String query = "UPDATE series SET";
            // Vamos añadiendo los distintos valores que queremos cambiar con una flag para 
            // que no nos de un error con la query
            boolean aux = false;
            if (!new_name.equals("")) {
                query += " nombre = '" + new_name + "'";
                aux = true;
            }
            if (!new_director.equals("")) {
                if (aux) {
                    query += ",";
                }
                query += " director='" + new_director + "' ";
                aux = true;
            }
            if (new_platform != 0) {
                if (aux) {
                    query += ",";
                }
                query += " plataforma='" + String.valueOf(new_platform) + "' ";
                aux = true;
            }

            if (aux) {
                sta = connect.createStatement();

                query += " WHERE nombre like '" + old_name + "';";
                sta.executeUpdate(query);
                sta.close();
            }
        } catch (Exception e) {
        }

        try {
            Statement sta;
            String query = "UPDATE peliculas SET";
            // Vamos añadiendo los distintos valores que queremos cambiar con una flag para 
            // que no nos de un error con la query
            boolean aux = false;
            if (!new_name.equals("")) {
                query += " nombre = '" + new_name + "'";
                aux = true;
            }
            if (!new_director.equals("")) {
                if (aux) {
                    query += ",";
                }
                query += " director='" + new_director + "' ";
                aux = true;
            }
            if (new_platform != 0) {
                if (aux) {
                    query += ",";
                }
                query += " plataforma='" + String.valueOf(new_platform) + "' ";
                aux = true;
            }

            if (aux) {
                sta = connect.createStatement();

                query += " WHERE nombre like '" + old_name + "';";
                sta.executeUpdate(query);
                sta.close();
            }
        } catch (Exception e) {
        }
    }
}

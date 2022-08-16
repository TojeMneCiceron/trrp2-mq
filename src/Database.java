import ru.psu.movs.trrp.socketmq.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Database
{
    static final String DB_URL = "jdbc:postgresql://localhost:5432/vet1";
    static final String USER = "postgres";
    static final String PASS = "postgres";

    public static void DropTables()
    {
        System.out.println("Dropping tables...");
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL JDBC Driver is not found. Include it in your library path ");
            e.printStackTrace();
            return;
        }

        Connection connection = null;
        Statement statement = null;

        String commandText = """
                DROP SCHEMA public CASCADE;
                CREATE SCHEMA public;
                GRANT ALL ON SCHEMA public TO postgres;
                GRANT ALL ON SCHEMA public TO public;
        """;

        try {
            connection = DriverManager
                    .getConnection(DB_URL, USER, PASS);
            statement = connection.createStatement();

            statement.execute(commandText);

            System.out.println("Tables dropped");

        } catch (SQLException e) {
            System.out.println("Connection Failed");
            e.printStackTrace();
            return;
        }

        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void CreateTables()
    {
        System.out.println("Creating tables...");
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL JDBC Driver is not found. Include it in your library path ");
            e.printStackTrace();
            return;
        }

        Connection connection = null;
        Statement statement = null;

        String commandText = """
        create table IF NOT EXISTS species
        (
                id serial not null
                constraint species_pk
                primary key,
                name text not null
                                    );

        create unique index species_name_uindex
        on species (name);

        create table IF NOT EXISTS owners
        (
                id serial not null
        constraint owners_pk
        primary key,
        name text not null,
            phone text not null
                                    );

        create unique index owners_phone_uindex
        on owners (phone);

        create table IF NOT EXISTS diagnosis
        (
                id serial not null
        constraint diagnosis_pk
        primary key,
        name text not null,
            description text not null
                                    );

        create unique index diagnosis_name_uindex
        on diagnosis (name);

        create table IF NOT EXISTS pets
        (
                id serial not null
        constraint pets_pk
        primary key,
        name text not null,
            age int not null,
            owner int
        constraint pets_owners_id_fk
        references owners (id),
            species int not null
        constraint pets_species_id_fk
        references species (id)
                                    );

        create unique index pets_name_age_species_uindex
        on pets (name, age, owner, species);

        create table IF NOT EXISTS pnd
        (
                pet_id int not null,
                diagnosis_id int not null,
                    constraint pnd_pk
                        unique (pet_id, diagnosis_id)
        );""";

        try {
            connection = DriverManager
                    .getConnection(DB_URL, USER, PASS);
            statement = connection.createStatement();

            statement.execute(commandText);

            System.out.println("Tables created");

        } catch (SQLException e) {
            System.out.println("Connection Failed");
            e.printStackTrace();
            return;
        }

        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void WriteRow(Row row) {
        System.out.println("Writing row...");
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL JDBC Driver is not found. Include it in your library path ");
            e.printStackTrace();
            return;
        }

        Connection connection = null;
//        Statement statement = null;

        try {
            connection = DriverManager
                    .getConnection(DB_URL, USER, PASS);

            var statement = connection.prepareStatement("""
                     INSERT INTO diagnosis (name, description) VALUES (?, ?) ON CONFLICT DO NOTHING;
                                                    
                     INSERT INTO species (name) VALUES (?) ON CONFLICT DO NOTHING;
                                   
                     INSERT INTO owners (name, phone) VALUES (?, ?) ON CONFLICT DO NOTHING;
                                   
                     INSERT INTO pets (name, age, owner, species) VALUES (?, ?, (SELECT id from owners WHERE owners.phone = ?), (SELECT id from species WHERE species.name = ?)) ON CONFLICT DO NOTHING;
                                   
                     INSERT INTO pnd VALUES ((SELECT id from pets WHERE pets.name = ? AND pets.age = ? AND pets.owner = (SELECT id from owners WHERE owners.phone = ?) AND pets.species = (SELECT id from species WHERE species.name = ?)), (SELECT id from diagnosis WHERE diagnosis.name = ?)) ON CONFLICT DO NOTHING;
                    """);
            statement.setString(1, row.d_name);
            statement.setString(2, row.description);
            statement.setString(3, row.s_name);
            statement.setString(4, row.o_name);
            statement.setString(5, row.phone);
            statement.setString(6, row.p_name);
            statement.setInt(7, row.age);
            statement.setString(8, row.phone);
            statement.setString(9, row.s_name);
            statement.setString(10, row.p_name);
            statement.setInt(11, row.age);
            statement.setString(12, row.phone);
            statement.setString(13, row.s_name);
            statement.setString(14, row.d_name);

            statement.executeUpdate();

            statement.close();

            System.out.println("Row is written");
        } catch (SQLException e) {
            System.out.println("Connection Failed");
            e.printStackTrace();
            return;
        }

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

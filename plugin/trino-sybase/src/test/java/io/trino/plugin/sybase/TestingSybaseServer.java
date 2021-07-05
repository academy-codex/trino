package io.trino.plugin.sybase;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestingSybaseServer
        extends JdbcDatabaseContainer<TestingSybaseServer>
{

    public static final String DEFAULT_TAG = "datagrip/sybase";
    private static final int SYBASE_PORT = 5000;

    public TestingSybaseServer() {this(DEFAULT_TAG);}

    public TestingSybaseServer(String dockerImageName)
    {
        super(DockerImageName.parse(dockerImageName));
//        addEnv("ROOT_PASSWORD", "memsql_root_password");
//        withCommand("sh", "-xeuc",
//                "/startup && " +
//                        // Lower the size of pre-allocated log files to 1MB (minimum allowed) to reduce disk footprint
//                        "memsql-admin update-config --yes --all --set-global --key \"log_file_size_partitions\" --value \"1048576\" && " +
//                        "memsql-admin update-config --yes --all --set-global --key \"log_file_size_ref_dbs\" --value \"1048576\" && " +
//                        // re-execute startup to actually start the nodes (first run performs setup but doesn't start the nodes)
//                        "exec /startup");
        start();
    }

    @Override
    public String getDriverClassName()
    {
        return "net.sourceforge.jtds.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl()
    {
        return "jdbc:jtds:sybase://" + getContainerIpAddress() + ":" + getMappedPort(SYBASE_PORT) + "/testdb";
    }

    @Override
    public String getUsername()
    {
        return "sa";
    }

    @Override
    public String getPassword()
    {
        return "myPassword";
    }

    @Override
    protected String getTestQueryString()
    {
        return "SELECT 1";
    }

    public void execute(String sql)
    {
        execute(sql, getUsername(), getPassword());
    }

    public void execute(String sql, String user, String password)
    {
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), user, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

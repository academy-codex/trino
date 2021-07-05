package io.trino.plugin.sybase;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestSybasePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new SybasePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:mariadb://test"), new TestingConnectorContext()).shutdown();
    }
}

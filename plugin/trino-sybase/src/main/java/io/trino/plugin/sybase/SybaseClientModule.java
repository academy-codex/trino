/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.sybase;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import net.sourceforge.jtds.jdbc.Driver;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.sybase.SybaseClient.SYBASE_MAX_LIST_EXPRESSIONS;

public class SybaseClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SybaseClient.class).in(Scopes.SINGLETON);
//        bindTablePropertiesProvider(binder, SqlServerTableProperties.class);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SYBASE_MAX_LIST_EXPRESSIONS);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        return new DriverConnectionFactory(new Driver(), config, credentialProvider);
    }
}

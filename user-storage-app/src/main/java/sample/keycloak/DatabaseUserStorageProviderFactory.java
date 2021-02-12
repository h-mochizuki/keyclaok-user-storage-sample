package sample.keycloak;

import org.jboss.logging.Logger;
import org.keycloak.common.util.EnvUtil;
import org.keycloak.component.ComponentModel;
import org.keycloak.component.ComponentValidationException;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.storage.UserStorageProviderFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * データベースを使用して認証するユーザストレージプロバイダのファクトリ
 */
public class DatabaseUserStorageProviderFactory implements
        UserStorageProviderFactory<DatabaseUserStorageProvider> {

    // ロガー
    private static final Logger LOG = Logger.getLogger(DatabaseUserStorageProviderFactory.class);
    // プロバイダ名
    private static final String PROVIDER_NAME = "database-user-storage";
    // プロバイダ追加設定項目リスト
    private static final List<ProviderConfigProperty> configMetadata;
    // 設定項目ID: DB接続URL
    private static final String CONFIG_URL = "Url";
    // 設定項目ID: DB接続ユーザ
    private static final String CONFIG_USERNAME = "Username";
    // 設定項目ID: DB接続パスワード
    private static final String CONFIG_PASSWORD = "Password";
    // 設定項目ID: ユーザ検索SQL
    private static final String CONFIG_SQL = "Sql";

    static {
        configMetadata = ProviderConfigurationBuilder.create()
                .property().name(CONFIG_URL)
                .label(CONFIG_URL)
                .type(ProviderConfigProperty.STRING_TYPE)
                .helpText("データベース接続URL")
                .defaultValue("jdbc:postgresql://localhost:5432/postgres")
                .add()
                .property().name(CONFIG_USERNAME)
                .label(CONFIG_USERNAME)
                .type(ProviderConfigProperty.STRING_TYPE)
                .helpText("データベース接続ユーザ")
                .defaultValue("postgres")
                .add()
                .property().name(CONFIG_PASSWORD)
                .label(CONFIG_PASSWORD)
                .type(ProviderConfigProperty.STRING_TYPE)
                .helpText("データベース接続パスワード")
                .defaultValue("postgres")
                .add()
                .property().name(CONFIG_SQL)
                .label(CONFIG_SQL)
                .type(ProviderConfigProperty.STRING_TYPE)
                .helpText("ユーザ検索用SQL\n${username}がユーザ名に置換されます")
                .defaultValue("select username from pg_user where username = '${username}'")
                .add()
                .build();
    }

    /**
     * プロバイダ識別子を返します
     *
     * @return プロバイダ識別子
     */
    @Override
    public String getId() {
        return PROVIDER_NAME;
    }

    /**
     * 設定項目リストを返します
     *
     * @return 設定項目リスト
     */
    @Override
    public List<ProviderConfigProperty> getConfigProperties() {
        return configMetadata;
    }

    /**
     * 設定入力内容をチェックします
     *
     * @param session セッション情報
     * @param realm   レルム
     * @param config  設定内容
     * @throws ComponentValidationException 設定内容に問題があった場合の例外
     */
    @Override
    public void validateConfiguration(
            KeycloakSession session,
            RealmModel realm,
            ComponentModel config) throws ComponentValidationException {
        String url = requiredValue(config, CONFIG_URL);
        String username = requiredValue(config, CONFIG_USERNAME);
        String password = requiredValue(config, CONFIG_PASSWORD);
        requiredValue(config, CONFIG_SQL);
        testConnection(url, username, password);
    }

    /**
     * 入力内容があるかをチェックした後、入力内容を返します
     *
     * @param config 設定内容
     * @param key 入力内容取得のキー
     * @return 入力内容
     * @throws ComponentValidationException 入力がなかった場合の例外
     */
    private String requiredValue(ComponentModel config, String key)
            throws ComponentValidationException {
        String value = value(config, key);
        if (value == null || value.isBlank()) {
            throw new ComponentValidationException(
                    String.format("%s is required.", key));
        }
        return EnvUtil.replace(value);
    }

    /**
     * 設定内容から入力内容を取得して返します
     *
     * @param config 設定内容
     * @param key 入力内容取得のキー
     * @return 入力内容
     */
    private String value(ComponentModel config, String key) {
        String value = config.getConfig().getFirst(key);
        return value == null || value.isBlank() ? value : EnvUtil.replace(value);
    }

    /**
     * 指定データベースに接続できるかをテストします
     *
     * @param url データベースURL
     * @param username 接続ユーザ名
     * @param password 接続パスワード
     * @throws ComponentValidationException DB接続例外
     */
    private void testConnection(String url, String username, String password)
            throws ComponentValidationException {
        try {
            DriverManager.getConnection(url, username, password);
            LOG.debugv("Connection succeeded: url={0}, username={1}", url, username);
        } catch (SQLException e) {
            LOG.error("Connection refused.", e);
            throw new ComponentValidationException("Connection refused.", e);
        }
    }

    /**
     * プロバイダを生成します
     *
     * @param session セッション情報
     * @param model   プロバイダ設定内容
     * @return データベースを使用して認証するユーザストレージプロバイダ
     */
    @Override
    public DatabaseUserStorageProvider create(KeycloakSession session, ComponentModel model) {
        try {
            String url = requiredValue(model, CONFIG_URL);
            String username = requiredValue(model, CONFIG_USERNAME);
            String password = requiredValue(model, CONFIG_PASSWORD);
            return new DatabaseUserStorageProvider(session, model,
                    DriverManager.getConnection(url, username, password),
                    requiredValue(model, CONFIG_SQL));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

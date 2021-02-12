package sample.keycloak;

import org.apache.commons.lang.text.StrSubstitutor;
import org.jboss.logging.Logger;
import org.keycloak.component.ComponentModel;
import org.keycloak.credential.CredentialInput;
import org.keycloak.credential.CredentialInputUpdater;
import org.keycloak.credential.CredentialInputValidator;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.storage.ReadOnlyException;
import org.keycloak.storage.StorageId;
import org.keycloak.storage.UserStorageProvider;
import org.keycloak.storage.adapter.AbstractUserAdapter;
import org.keycloak.storage.user.UserLookupProvider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * データベースに接続し、ユーザ認証を行うストレージプロバイダ
 */
public class DatabaseUserStorageProvider implements
        UserStorageProvider, UserLookupProvider, CredentialInputValidator, CredentialInputUpdater {

    /** ユーザ名を格納するキー */
    private static final String KEY_USERNAME = "username";
    // ロガー
    private static final Logger LOG = Logger.getLogger(DatabaseUserStorageProvider.class);
    // keycloakランタイムへのアクセスを提供するオブジェクト
    private final KeycloakSession session;
    // プロバイダの設定内容を保持するオブジェクト
    private final ComponentModel model;
    // SQLコネクション
    private final Connection connection;
    // ログインしたユーザのキャッシュ(再検索防止用)
    private final Map<String, UserModel> loginUserCache = new HashMap<>();
    // ユーザ検索用SQL
    private final String sql;

    /**
     * コンストラクタ
     *
     * @param session    セッション情報
     * @param model      プロバイダ設定内容
     * @param connection SQLコネクション
     * @param sql        ユーザ検索用SQL
     */
    public DatabaseUserStorageProvider(
            KeycloakSession session,
            ComponentModel model,
            Connection connection,
            String sql) {
        this.session = session;
        this.model = model;
        this.connection = connection;
        this.sql = sql;
    }

    /**
     * 例外を投げられる Function インターフェース
     *
     * @param <P> パラメータの型
     * @param <R> 戻り値の型
     * @param <T> 例外の型
     */
    private interface ThrowableFunction<P, R, T extends Throwable> {
        R apply(P param) throws T;
    }

    /**
     * 明示的なトランザクション境界内で処理を実施します
     *
     * @param function 実施したい処理
     * @param <T>      戻り値の型
     * @return 処理の結果
     */
    private <T> T transactionTry(
            ThrowableFunction<Statement, T, SQLException> function) {
        try (Statement st = connection.createStatement()) {
            return function.apply(st);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ユーザIDから認証用ユーザ情報を検索します
     *
     * @param id    ユーザID
     * @param realm レルム
     * @return 認証用ユーザ情報
     */
    @Override
    public UserModel getUserById(String id, RealmModel realm) {
        StorageId storageId = new StorageId(id);
        String username = storageId.getExternalId();
        return getUserByUsername(username, realm);
    }

    /**
     * ユーザ名から認証用ユーザ情報を検索します
     *
     * @param username ユーザ名
     * @param realm    レルム
     * @return 認証用ユーザ情報
     */
    @Override
    public UserModel getUserByUsername(String username, RealmModel realm) {
        UserModel adapter = loginUserCache.get(username);
        if (adapter == null) {
            try {
                Map<String, String> param = new HashMap<>();
                param.put(KEY_USERNAME, username);
                String name = transactionTry(st -> {
                    ResultSet re = st.executeQuery(new StrSubstitutor(param).replace(sql));
                    return re.next() ? re.getString(KEY_USERNAME) : null;
                });
                return name == null || name.isBlank() ? null : createAdapter(name, realm);
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.warnv(e, "Unable to search for '{0}'", username);
                } else {
                    LOG.warnv("Unable to search for '{0}'", username);
                }
            }
        }
        return adapter;
    }

    /**
     * 認証用ユーザ情報を作成します
     *
     * @param username ユーザ名
     * @param realm    レルム
     * @return 認証用ユーザ情報
     */
    protected UserModel createAdapter(String username, RealmModel realm) {
        return new AbstractUserAdapter(session, realm, model) {
            @Override
            public String getUsername() {
                return username;
            }
        };
    }

    /**
     * メールアドレスから認証用ユーザ情報を検索します
     *
     * @param email メールアドレス
     * @param realm レルム
     * @return 認証用ユーザ情報
     */
    @Override
    public UserModel getUserByEmail(String email, RealmModel realm) {
        // TODO メールアドレスから検索
        // TODO ヒットしたらユーザ名から認証用ユーザ情報を作成
        return null;
    }

    /**
     * 想定している認証形式かを判定します
     *
     * @param realm          レルム
     * @param user           認証用ユーザ情報
     * @param credentialType 認証形式
     * @return true:想定した認証形式<br>false:想定外の認証形式
     */
    @Override
    public boolean isConfiguredFor(RealmModel realm, UserModel user, String credentialType) {
        // TODO 認証を考える
        return true;
    }

    /**
     * 認証検証します
     *
     * @param realm           レルム
     * @param user            認証用ユーザ情報
     * @param credentialInput 認証のために入力された内容
     * @return true:認証OK<br>false:認証NG
     */
    @Override
    public boolean isValid(RealmModel realm, UserModel user, CredentialInput credentialInput) {
        // TODO 認証を考える
        loginUserCache.put(user.getUsername(), user);
        if (LOG.isDebugEnabled()) {
            LOG.debugv("Success for connecting: {0}", user.getUsername());
        }
        return true;
    }

    /**
     * 対象ユーザの認証情報を更新します
     *
     * @param realm レルム
     * @param user  認証ユーザ情報
     * @param input 認証更新入力内容
     * @return true:ユーザ認証情報が更新された<br>false:更新されなかった
     */
    @Override
    public boolean updateCredential(RealmModel realm, UserModel user, CredentialInput input) {
        // 更新は許可しない
        throw new ReadOnlyException("User is read only for this update");
    }

    /**
     * 対象ユーザの認証形式を無効にします
     *
     * @param realm          レルム
     * @param user           認証ユーザ情報
     * @param credentialType 認証形式
     */
    @Override
    public void disableCredentialType(RealmModel realm, UserModel user, String credentialType) {
        // TODO 認証を考える
    }

    /**
     * 認証形式をサポートしているかを返します
     *
     * @param credentialType 認証形式
     * @return true:サポートしている<br>false:サポートしていない
     */
    @Override
    public boolean supportsCredentialType(String credentialType) {
        // TODO 認証を考える
        return true;
    }

    /**
     * 対象ユーザの無効とする認証形式一覧を返します
     *
     * @param realm レルム
     * @param user  認証用ユーザ情報
     * @return 無効な認証形式一覧
     */
    @Override
    public Set<String> getDisableableCredentialTypes(RealmModel realm, UserModel user) {
        // TODO 認証を考える
        return Collections.emptySet();
    }

    /**
     * プロバイダーを解放します
     */
    @Override
    public void close() {
        try {
            connection.close();
        } catch (Exception ignore) {
        }
    }
}

//! Connection parameters
use std::error::Error;
use std::path::PathBuf;

use url::{self, Url};

/// Specifies the target server to connect to.
#[derive(Clone, Debug, PartialEq)]
pub enum ConnectTarget {
    /// Connect via TCP to the specified host.
    Tcp(String),
    /// Connect via a Unix domain socket in the specified directory.
    ///
    /// Unix sockets are only supported on Unixy platforms (i.e. not Windows).
    Unix(PathBuf),
}

/// Authentication information.
#[derive(Clone, Debug, PartialEq)]
pub struct UserInfo {
    /// The username.
    pub user: String,
    /// An optional password.
    pub password: Option<String>,
}

/// Information necessary to open a new connection to a Postgres server.
#[derive(Clone, Debug)]
pub struct ConnectParams {
    /// The target server.
    pub target: ConnectTarget,
    /// The target port.
    ///
    /// Defaults to 5432 if not specified.
    pub port: Option<u16>,
    /// The user to login as.
    ///
    /// `Connection::connect` requires a user but `cancel_query` does not.
    pub user: Option<UserInfo>,
    /// The database to connect to.
    ///
    /// Defaults the value of `user`.
    pub database: Option<String>,
    /// Runtime parameters to be passed to the Postgres backend.
    pub options: Vec<(String, String)>,
}

/// A trait implemented by types that can be converted into a `ConnectParams`.
pub trait IntoConnectParams {
    /// Converts the value of `self` into a `ConnectParams`.
    fn into_connect_params(self) -> Result<ConnectParams, Box<Error + Sync + Send>>;
}

impl IntoConnectParams for ConnectParams {
    fn into_connect_params(self) -> Result<ConnectParams, Box<Error + Sync + Send>> {
        Ok(self)
    }
}

impl<'a> IntoConnectParams for &'a str {
    fn into_connect_params(self) -> Result<ConnectParams, Box<Error + Sync + Send>> {
        match Url::parse(self) {
            Ok(url) => url.into_connect_params(),
            Err(err) => Err(err.into()),
        }
    }
}

impl IntoConnectParams for Url {
    fn into_connect_params(self) -> Result<ConnectParams, Box<Error + Sync + Send>> {
        let Url { host, port, user, path: url::Path { mut path, query: options, .. }, .. } = self;

        let maybe_path = try!(url::decode_component(&host));
        let target = if maybe_path.starts_with('/') {
            ConnectTarget::Unix(PathBuf::from(maybe_path))
        } else {
            ConnectTarget::Tcp(host)
        };

        let user = user.map(|url::UserInfo { user, pass }| {
            UserInfo {
                user: user,
                password: pass,
            }
        });

        let database = if path.is_empty() {
            None
        } else {
            // path contains the leading /
            path.remove(0);
            Some(path)
        };

        Ok(ConnectParams {
            target: target,
            port: port,
            user: user,
            database: database,
            options: options,
        })
    }
}

pub struct DynamicParams {
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    options: Vec<(String, String)>,
}

impl DynamicParams {
    pub fn new() -> Self {
        DynamicParams{ host: None, port: None, user: None, password: None, database: None, options: Vec::new() }
    }

    pub fn user<S>(mut self, user: S) -> Self where S: Into<String> {
        self.user = Some(user.into());
        self
    }

    pub fn password<S>(mut self, password: S) -> Self where S: Into<String> {
        self.password = Some(password.into());
        self
    }
    // Convenience methods
    pub fn username<S>(mut self, user: S) -> Self where S: Into<String> { self.user(user) }
    pub fn pass<S>(mut self, pass: S) -> Self where S: Into<String> { self.password(pass) }

    pub fn database<S>(mut self, database: S) -> Self where S: Into<String> {
        self.database = Some(database.into());
        self
    }

    pub fn host<S>(mut self, host: S) -> Self where S: Into<String> {
        self.host = Some(host.into());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn option<S>(mut self, k: S, v: S) -> Self where S: Into<String> {
        self.options.push((k.into(), v.into()));
        self
    }
}

impl IntoConnectParams for DynamicParams {
    fn into_connect_params(self) -> Result<ConnectParams, Box<Error + Sync + Send>> {
        let user = try!(self.user.ok_or("Must specify username".to_string()));
        let userinfo = UserInfo {
                user: user,
                password: self.password,
        };

        let target = match self.host {
            None => ConnectTarget::Unix(PathBuf::from(format!("/var/run/postgresql/.s.PGSQL.{}", self.port.unwrap_or(5432)))),
            Some(h) => ConnectTarget::Tcp(h),
        };
        let port: Option<u16> = self.port;
        let database = self.database;

        Ok(ConnectParams {
            target: target,
            port: port,
            user: Some(userinfo),
            database: database,
            options: self.options,
        })
        
    }
}


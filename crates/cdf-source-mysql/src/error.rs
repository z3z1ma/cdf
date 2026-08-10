use cdf_kernel::{CdfError, ErrorKind};

pub(crate) fn classify_mysql_error(action: &str, error: mysql_async::Error) -> CdfError {
    use mysql_async::Error;

    let message = match &error {
        Error::Server(server) => format!(
            "{action}: MySQL server error {} ({})",
            server.code, server.state
        ),
        Error::Driver(driver) => format!("{action}: MySQL driver error: {driver}"),
        Error::Io(io) => format!("{action}: MySQL transport error: {io}"),
        Error::Url(_) => format!("{action}: MySQL connection URI is invalid"),
        Error::Other(_) => format!("{action}: MySQL client error"),
    };
    let kind = match &error {
        Error::Server(server) if matches!(server.code, 1044 | 1045 | 1142 | 1227) => {
            ErrorKind::Auth
        }
        Error::Server(server) if matches!(server.code, 1064 | 1146 | 1054 | 1235) => {
            ErrorKind::Data
        }
        Error::Url(_) => ErrorKind::Auth,
        Error::Io(_) => ErrorKind::Transient,
        Error::Driver(_) | Error::Other(_) | Error::Server(_) => ErrorKind::Data,
    };
    CdfError::new(kind, message)
}

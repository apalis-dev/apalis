#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub(crate) enum Error {
    #[error("Task panicked: {0}")]
    Panic(String),

    #[error("Invalid email")]
    InvalidEmail,

    #[error("Api ratelimit")]
    ApiRateLimit,
}

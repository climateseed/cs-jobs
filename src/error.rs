use uuid::Uuid;

use crate::job::Status;
use crate::job_queue::Message;

/// Macro used to box an error to fit the functions return `ApiError`.
#[macro_export]
macro_rules! api_err {
    ($e: expr) => {
        Box::new($e)
    };
}

/// Alias to be used for functions returns.
pub type ApiError = Box<Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Queue is already running")]
    AlreadyRunning,
    #[error("Cannot access error handler ({0})")]
    CannotAccessErrorHandler(String),
    #[error("Cannot access expirations list ({0})")]
    CannotAccessExpirations(String),
    #[error("Cannot access receiver ({0})")]
    CannotAccessReceiver(String),
    #[error("Cannot access runtime ({0})")]
    CannotAccessRuntime(String),
    #[error("Cannot access sender ({0})")]
    CannotAccessSender(String),
    #[error("Cannot join the queue thread")]
    CannotJoinThread,
    #[error("Cannot send message to the queue ({0})")]
    CannotSendMessage(String),
    #[error("{0}")]
    Custom(String),
    #[error(transparent)]
    GenericError(#[from] Box<dyn std::error::Error>),
    #[error("Invalid job status")]
    InvalidJobStatus,
    #[error("Invalid job status transition: {0:?}")]
    InvalidJobStatusTransition((Status, Status)),
    #[error("Invalid thread pool size")]
    InvalidThreadPoolSize,
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("The job cannot be removed as it's not finished")]
    JobNotFinished,
    #[error("Job with id {0} is not found")]
    JobNotFound(Uuid),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    JsonSerialization(#[from] serde_json::Error),
    #[error(transparent)]
    MessageSend(#[from] std::sync::mpsc::SendError<Message>),
    #[error("Missing channel for communicating with thread")]
    MissingChannel,
    #[error("Missing thread's join handle")]
    MissingJoinHandle,
    #[error("Missing notification handler")]
    MissingNotificationHandler,
    #[error("Missing private data in job")]
    MissingPrivateData,
    #[error("Queue is not started")]
    NotStarted,
    #[error("Queue is not stopping")]
    NotStopping,
    #[error("Progression overflow")]
    ProgressionOverflow,
    #[error("Queue is stopped")]
    Stopped,
    #[error("Error during waiting for timeout ({0})")]
    Timeout(String),
}

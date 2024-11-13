use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::prelude::*;

/// Type used to share the backend instance across threads.
pub type SharedBackend<Routine, Context> = Arc<Mutex<Box<dyn Backend<Routine, Context>>>>;

/// Backend trait that defines the behavior of the backend that is responsible for storing the job
/// and their results.
#[async_trait]
pub trait Backend<Routine, Context>: Send {
    /// Get a job.
    ///
    /// # Arguments
    /// * `id` - Job identifier to be run.
    ///
    /// # Returns
    /// The job instance.
    ///
    /// # Errors
    /// One of `Error` enum.
    async fn get(&mut self, id: &Uuid) -> Result<Job, ApiError>;

    /// Schedule a job to be processed.
    ///
    /// # Arguments
    /// * `job` - Job structure to be processed.
    ///
    /// # Errors
    /// One of `Error` enum.
    fn schedule(&mut self, job: Job) -> Result<(), ApiError>;

    /// Run a job.
    ///
    /// # Arguments
    /// * `id` - Job identifier to be run.
    /// * `context` - Context maybe needed by the jobs.
    /// * `messages_channel` - Channel used to send messages to the queue.
    ///
    /// # Errors
    /// One of `Error` enum.
    async fn run(
        &mut self,
        id: &Uuid,
        context: Option<Shared<Context>>,
        messages_channel: SharedMessageChannel,
    ) -> Result<(), ApiError>;

    /// Get the status of a job.
    ///
    /// # Arguments
    /// * `id` - Job identifier to be fetched.
    ///
    /// # Returns
    /// The status of the job.
    ///
    /// # Errors
    /// One of `Error` enum.
    fn status(&self, id: &Uuid) -> Result<Status, ApiError>;

    /// Set the status of a job.
    ///
    /// # Arguments
    /// * `id` - Job identifier to be modified.
    /// * `status` - Status to be set.
    ///
    /// # Errors
    /// One of `Error` enum.
    fn set_status(&mut self, id: &Uuid, status: Status) -> Result<(), ApiError>;

    /// Get the result of a job.
    ///
    /// # Arguments
    /// * `id` - Job identifier to be fetched.
    ///
    /// # Returns
    /// The result of the job as list of bytes.
    ///
    /// # Errors
    /// One of `Error` enum.
    fn result(&self, id: &Uuid) -> Result<&[u8], ApiError>;

    /// Set the result of a job.
    ///
    /// # Arguments
    /// * `id` - Job identifier to be modified.
    /// * `result` - Result to be set.
    ///
    /// # Errors
    /// One of `Error` enum.
    fn set_result(&mut self, id: &Uuid, result: Vec<u8>) -> Result<(), ApiError>;

    /// Set the number of steps for a job.
    ///
    /// # Arguments
    /// * `id` - Job identifier to be modified.
    /// * `steps` - Number of steps to set.
    ///
    /// # Returns
    /// Current progression.
    ///
    /// # Errors
    /// One of `Error` enum.
    fn set_steps(&mut self, id: &Uuid, steps: u64) -> Result<Progression, ApiError>;

    /// Set the current step for a job.
    ///
    /// # Arguments
    /// * `id` - Job identifier to be modified.
    /// * `step` - Current step to set.
    ///
    /// # Returns
    /// Current progression.
    ///
    /// # Errors
    /// One of `Error` enum.
    fn set_step(&mut self, id: &Uuid, step: u64) -> Result<Progression, ApiError>;

    /// Get the progression of a job.
    ///
    /// # Arguments
    /// * `id` - Job identifier to be fetched.
    ///
    /// # Returns
    /// The progression of the job as `Progression`.
    ///
    /// # Errors
    /// One of `Error` enum.
    fn progression(&self, id: &Uuid) -> Result<Progression, ApiError>;

    /// Get the expire policy of a job.
    ///
    /// # Arguments
    /// * `id` - Job identifier to be fetched.
    ///
    /// # Returns
    /// The `ExpirePolicy` of the job.
    ///
    /// # Errors
    /// One of `Error` enum.
    fn expire_policy(&self, id: &Uuid) -> Result<ExpirePolicy, ApiError>;

    /// Remove a job.
    ///
    /// # Arguments:
    /// * `id` - Job identifier to be removed.
    ///
    /// # Errors
    /// One of `Error` enum.
    fn remove(&mut self, id: &Uuid) -> Result<(), ApiError>;

    /// Remove all expired jobs.
    ///
    /// # Returns
    /// The list of job IDs removed.
    ///
    /// # Errors
    /// One of `Error` enum.
    fn remove_expired(&mut self) -> Result<Vec<Uuid>, ApiError>;

    /// Get the list of all jobs.
    ///
    /// # Returns
    /// The list of jobs
    ///
    /// # Errors
    /// One of `Error` enum.
    fn jobs(&self) -> Result<Vec<Job>, ApiError>;
}

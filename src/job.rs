use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

use crate::prelude::*;

lazy_static! {
    /// Random data used to generate UUID V1 values.
    static ref GROUP_ID: [u8; 6] = rand::thread_rng().gen::<[u8; 6]>();
}

/// List of expiry configurations available for a job.
#[derive(Clone, Copy, Debug, Default, PartialEq, Deserialize, Serialize)]
pub enum ExpirePolicy {
    /// The job is only removed manually.
    #[default]
    Manual,

    /// The job is removed once it's result is fetched.
    /// Force the user to provide a timeout to avoid storing ad vitam aeternam.
    OnResultFetch(Duration),

    /// The job is removed after a specified duration.
    Timeout(Duration),
}

/// List of result statuses of a job.
#[derive(Clone, Copy, Debug, Default, PartialEq, Deserialize, Serialize)]
pub enum ResultStatus {
    /// The job has finished successfully.
    #[default]
    Success,

    /// The job has finished in error.
    Error,
}

/// List of statuses of a job.
#[derive(Clone, Copy, Debug, Default, PartialEq, Deserialize, Serialize)]
pub enum Status {
    /// Job is not ready and not scheduled.
    #[default]
    NotReady,

    /// Job is not running yet but ready to be started.
    Ready,

    /// Job is currently running.
    Running,

    /// Job is finished (on success or error).
    Finished(ResultStatus),

    /// Job has been removed from the job queue.
    Removed,
}

/// Structure used to store the progression steps of the job.
#[derive(Debug, Deserialize, Serialize)]
pub struct Progression {
    /// Current step.
    pub step: u64,

    /// Number of steps.
    pub steps: u64,
}

/// Structure used to store timestamps and result of the job.
#[derive(PartialEq, Deserialize, Serialize)]
pub struct Payload {
    /// Timestamps for every step of the job.
    pub timestamps: Timestamps,

    /// Result of the job.
    pub result: Vec<u8>,
}

/// Timestamps of every steps of the lifecycle of a job.
#[derive(PartialEq, Deserialize, Serialize)]
pub struct Timestamps {
    /// Timestamp at which the job is enqueued.
    pub enqueued: SystemTime,

    /// Timestamp at which the job is started.
    pub started: SystemTime,

    /// Timestamp at which the job is finished.
    pub finished: SystemTime,

    /// Timestamp at which the job is considered expired.
    pub expired: Option<SystemTime>,
}

/// Trait that must be derived for the list of possible routines handled by the jobs.
#[async_trait]
pub trait Routine<Context>: for<'a> Deserialize<'a> + Serialize + Send {
    /// Function that is called when the job is processed.
    ///
    /// # Arguments
    /// * `job_id` - ID of the job this routine belongs to.
    /// * `messages_channel` - Channel to be used to send back messages to the job queue.
    ///
    /// # Returns
    /// A list of bytes: the result of the job to be stored.
    ///
    /// # Errors
    /// One of `Error` enum.
    async fn call(
        &self,
        job: &Job,
        messages_channel: SharedMessageChannel,
        context: Option<Shared<Context>>,
    ) -> Result<Vec<u8>, Error>;
}

/// Description of a job.
#[derive(PartialEq, Deserialize, Serialize)]
pub struct Job {
    /// Unique identifier of the job.
    id: Uuid,

    /// The routine called when running.
    routine: String,

    /// Status of the job.
    status: Status,

    /// Payload of the job where the result will be stored.
    payload: Payload,

    /// Number of steps of the job.
    steps: u64,

    /// Current step (progression).
    step: u64,

    /// Expire policy for this job.
    expire_policy: ExpirePolicy,

    /// Private data.
    private_data: Option<String>,
}

impl Job {
    /// Creates a new job given a routine to be executed.
    /// The job will be only removed by a user call to `JobQueue::remove_job`.
    ///
    /// # Arguments
    /// * `routine` - Routine to be called.
    ///
    /// # Returns
    /// An `Job` instance.
    ///
    /// # Errors
    /// One of `Error` enum.
    pub fn new<Context>(routine: impl Routine<Context>) -> Result<Self, ApiError> {
        Self::new_with_expire(routine, ExpirePolicy::default())
    }

    /// Creates a new job given a routine to be executed and an expire policy.
    ///
    /// # Arguments
    /// * `routine` - Routine to be called.
    /// * `expire_policy` - Policy to be applied for job removal.
    ///
    /// # Returns
    /// An `Job` instance.
    ///
    /// # Errors
    /// One of `Error` enum.
    pub fn new_with_expire<Context>(
        routine: impl Routine<Context>,
        expire_policy: ExpirePolicy,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            id: Uuid::now_v1(&GROUP_ID),
            routine: serde_json::to_string(&routine).map_err(|e| api_err!(e.into()))?,
            status: Status::NotReady,
            payload: Payload {
                timestamps: Timestamps {
                    enqueued: SystemTime::now(),
                    started: SystemTime::UNIX_EPOCH,
                    finished: SystemTime::UNIX_EPOCH,
                    expired: None,
                },
                result: vec![],
            },
            steps: 0,
            step: 0,
            expire_policy,
            private_data: None,
        })
    }

    /// Get the unique identifier of the job.
    ///
    /// # Returns
    /// The ID of the job.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Get the status of the job.
    ///
    /// # Returns
    /// The current status of the job.
    pub fn status(&self) -> Status {
        self.status
    }

    /// Set the status of the job.
    ///
    /// # Arguments
    /// * ̀`status` - Value to be set.
    ///
    /// # Errors
    /// One of `Error` enum.
    pub fn set_status(&mut self, status: Status) -> Result<(), ApiError> {
        match status {
            Status::NotReady => {
                return Err(api_err!(Error::InvalidJobStatusTransition((
                    self.status,
                    status
                ))))
            }

            Status::Ready => {
                if self.status != Status::NotReady {
                    return Err(api_err!(Error::InvalidJobStatusTransition((
                        self.status,
                        status
                    ))));
                } else {
                    self.payload.timestamps.enqueued = SystemTime::now();
                }
            }

            Status::Running => {
                if self.status != Status::Ready {
                    return Err(api_err!(Error::InvalidJobStatusTransition((
                        self.status,
                        status
                    ))));
                } else {
                    self.payload.timestamps.started = SystemTime::now();
                }
            }

            Status::Finished(_) => {
                if self.status != Status::Running {
                    return Err(api_err!(Error::InvalidJobStatusTransition((
                        self.status,
                        status
                    ))));
                } else {
                    let now = SystemTime::now();

                    self.payload.timestamps.finished = now;

                    // If expire policy has a timeout, then store the time of expiration
                    match self.expire_policy {
                        ExpirePolicy::OnResultFetch(duration) | ExpirePolicy::Timeout(duration) => {
                            self.payload.timestamps.expired = Some(now + duration)
                        }
                        _ => (),
                    }
                }
            }

            _ => return Err(api_err!(Error::InvalidJobStatus)),
        }

        self.status = status;

        Ok(())
    }

    /// Get the result of the job.
    ///
    /// # Returns
    /// The result of the job.
    pub fn result(&self) -> &[u8] {
        &self.payload.result
    }

    /// Set the result of the job that will be stored as `serde_json::Value`.
    ///
    /// # Arguments
    /// * `value` - Serializable value to be stored.
    ///
    /// # Errors
    /// One of `Error` enum.
    pub fn set_result(&mut self, value: Vec<u8>) -> Result<(), ApiError> {
        self.payload.result = value;

        Ok(())
    }

    /// Set the total steps of the job.
    ///
    /// # Arguments
    /// * `steps` - Number of steps of the job.
    ///
    /// # Returns
    /// Current progression.
    ///
    /// # Errors
    /// One of `Error` enum.
    pub fn set_steps(&mut self, steps: u64) -> Result<Progression, ApiError> {
        if self.step <= steps {
            self.steps = steps;

            Ok(self.progression())
        } else {
            Err(api_err!(Error::ProgressionOverflow))
        }
    }

    /// Set the current step of the job.
    ///
    /// # Arguments
    /// * `step` - Current step of the job.
    ///
    /// # Returns
    /// Current progression.
    ///
    /// # Errors
    /// One of `Error` enum.
    pub fn set_step(&mut self, step: u64) -> Result<Progression, ApiError> {
        if step <= self.steps {
            self.step = step;

            Ok(self.progression())
        } else {
            Err(api_err!(Error::ProgressionOverflow))
        }
    }

    /// Get the progression of the job.
    ///
    /// # Returns
    /// The progression of the job.
    pub fn progression(&self) -> Progression {
        Progression {
            step: self.step,
            steps: self.steps,
        }
    }

    /// Get the expire policy of the job.
    ///
    /// # Returns
    /// The `ExpirePolicy` of the job.
    pub fn expire_policy(&self) -> ExpirePolicy {
        self.expire_policy
    }

    /// Check if the job is expired.
    ///
    /// # Returns
    ///  `true` if expired, `false`otherwise.
    pub fn is_expired(&self) -> bool {
        if let Some(expired) = self.payload.timestamps.expired {
            SystemTime::now() >= expired
        } else {
            false
        }
    }

    /// Get the private data owned by the job.
    ///
    /// # Returns
    /// The private data casted to the type provided.
    ///
    /// # Errors
    /// One of `Error` enum.
    pub fn private_data<T>(&self) -> Result<T, ApiError>
    where
        T: for<'a> Deserialize<'a> + Serialize,
    {
        let data = self.private_data.clone().ok_or(Error::MissingPrivateData)?;
        let data: T = serde_json::from_str(&data).map_err(|e| api_err!(e.into()))?;

        Ok(data)
    }

    /// Set the private data owned by the job.
    ///
    /// # Arguments
    /// * `value` - Serializable value to be stored.
    ///
    /// # Errors
    /// One of `Error` enum.
    pub fn set_private_data<'a>(
        &mut self,
        value: impl Deserialize<'a> + Serialize,
    ) -> Result<(), ApiError> {
        self.private_data = Some(serde_json::to_string(&value).map_err(|e| api_err!(e.into()))?);

        Ok(())
    }

    /// Call the underlying routine of the job.
    ///
    /// # Arguments
    /// * `messages_channel` - Channel used to send message to the job queue.
    ///
    /// # Errors
    /// One of `Error` enum.
    pub async fn run<T: Routine<Context>, Context>(
        &mut self,
        messages_channel: SharedMessageChannel,
        context: Option<Shared<Context>>,
    ) -> Result<(), ApiError> {
        // Routine information is stored as string so deserialize it
        let routine: T = serde_json::from_str(&self.routine).map_err(|e| api_err!(e.into()))?;

        // Call the routine
        let result = routine.call(self, messages_channel, context).await?;

        // Store the result
        self.set_result(result)
    }
}

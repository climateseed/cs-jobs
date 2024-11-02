use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::prelude::*;

/// A default backend implementation that stores everything in memory.
#[derive(Default)]
pub struct MemoryBackend {
    /// List of jobs stored and sorted by date added.
    jobs: BTreeMap<Uuid, Job>,
}

#[async_trait]
impl<RoutineType, Context> Backend<RoutineType, Context> for MemoryBackend
where
    RoutineType: Routine<Context> + Sync,
    for<'async_trait> Context: Send + 'async_trait,
{
    async fn get(&mut self, id: &Uuid) -> Result<Job, ApiError> {
        Ok(self
            .jobs
            .get(id)
            .ok_or(api_err!(Error::JobNotFound(id.to_owned())))?
            .to_owned())
    }

    fn schedule(&mut self, job: Job) -> Result<(), ApiError> {
        self.jobs.insert(job.id(), job);

        Ok(())
    }

    async fn run(
        &mut self,
        id: &Uuid,
        context: Option<Shared<Context>>,
        messages_channel: SharedMessageChannel,
    ) -> Result<(), ApiError> {
        if let Some(job) = self.jobs.get_mut(id) {
            job.run::<RoutineType, Context>(messages_channel, context)
                .await?;

            Ok(())
        } else {
            Err(api_err!(Error::JobNotFound(id.to_owned())))
        }
    }

    fn status(&self, id: &Uuid) -> Result<Status, ApiError> {
        Ok(self
            .jobs
            .get(id)
            .ok_or(api_err!(Error::JobNotFound(id.to_owned())))?
            .status())
    }

    fn set_status(&mut self, id: &Uuid, status: Status) -> Result<(), ApiError> {
        if let Some(job) = self.jobs.get_mut(id) {
            job.set_status(status)?;

            Ok(())
        } else {
            Err(api_err!(Error::JobNotFound(id.to_owned())))
        }
    }

    fn result(&self, id: &Uuid) -> Result<&[u8], ApiError> {
        Ok(self
            .jobs
            .get(id)
            .ok_or(api_err!(Error::JobNotFound(id.to_owned())))?
            .result())
    }

    fn set_result(&mut self, id: &Uuid, result: Vec<u8>) -> Result<(), ApiError> {
        if let Some(job) = self.jobs.get_mut(id) {
            job.set_result(result)?;

            Ok(())
        } else {
            Err(api_err!(Error::JobNotFound(id.to_owned())))
        }
    }

    fn set_steps(&mut self, id: &Uuid, steps: u64) -> Result<Progression, ApiError> {
        if let Some(job) = self.jobs.get_mut(id) {
            job.set_steps(steps)
        } else {
            Err(api_err!(Error::JobNotFound(id.to_owned())))
        }
    }

    fn set_step(&mut self, id: &Uuid, step: u64) -> Result<Progression, ApiError> {
        if let Some(job) = self.jobs.get_mut(id) {
            job.set_step(step)
        } else {
            Err(api_err!(Error::JobNotFound(id.to_owned())))
        }
    }

    fn progression(&self, id: &Uuid) -> Result<Progression, ApiError> {
        Ok(self
            .jobs
            .get(id)
            .ok_or(api_err!(Error::JobNotFound(id.to_owned())))?
            .progression())
    }

    fn expire_policy(&self, id: &Uuid) -> Result<ExpirePolicy, ApiError> {
        Ok(self
            .jobs
            .get(id)
            .ok_or(api_err!(Error::JobNotFound(id.to_owned())))?
            .expire_policy())
    }

    fn remove(&mut self, id: &Uuid) -> Result<(), ApiError> {
        let status = self
            .jobs
            .get(id)
            .ok_or(api_err!(Error::JobNotFound(id.to_owned())))?
            .status();

        match status {
            Status::Finished(_) => {
                self.jobs.remove(id);

                Ok(())
            }

            _ => Err(api_err!(Error::JobNotFinished)),
        }
    }

    fn remove_expired(&mut self) -> Result<Vec<Uuid>, ApiError> {
        let to_remove: Vec<_> = self
            .jobs
            .iter()
            .filter_map(|(job_id, job)| {
                if job.is_expired() {
                    Some(job_id.to_owned())
                } else {
                    None
                }
            })
            .collect();

        for job_id in &to_remove {
            self.jobs.remove(job_id);
        }

        Ok(to_remove)
    }

    fn jobs(&self) -> Result<Vec<Job>, ApiError> {
        Ok(self.jobs.values().cloned().collect())
    }
}

impl MemoryBackend {
    /// Creates a new instance of the memory backend.
    ///
    /// # Returns
    /// A instance of ̀̀ MemoryBackend`.
    pub fn new() -> Self {
        Self::default()
    }
}

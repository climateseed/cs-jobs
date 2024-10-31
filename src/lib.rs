pub mod backend;
pub mod error;
pub mod job;
pub mod job_queue;
pub mod job_queue_builder;
pub mod memory_backend;
pub mod prelude;
pub mod types;

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::Value;
    use std::sync::Mutex;
    use tokio::runtime::Runtime;

    use crate::prelude::*;

    static FLAG: Mutex<bool> = Mutex::new(false);

    struct Context {
        name: String,
    }

    #[derive(Serialize, Deserialize)]
    struct PrivateData {
        value: u8,
    }

    fn notification_handler(notification: Notification) {
        match notification {
            Notification::Error(e) => println!("ERR: {e}"),

            Notification::Progression(id, progression) => {
                println!("PROGRESSION({id}): {progression:#?}")
            }

            Notification::Status(id, status) => {
                println!("STATUS({id}): {status:#?}")
            }
        }
    }

    fn reset_flag() {
        set_flag(SetFlagArgs { value: false });
    }

    fn check_flag() {
        assert!(*FLAG.lock().unwrap());
    }

    fn set_flag(args: SetFlagArgs) {
        *FLAG.lock().unwrap() = args.value;
    }

    #[derive(Clone, Serialize, Deserialize)]
    struct SetFlagArgs {
        value: bool,
    }

    #[derive(Clone, Serialize, Deserialize)]
    struct CheckPrivateDataArgs {
        value: u8,
        expect_no_data: bool,
    }

    #[derive(Clone, Serialize, Deserialize)]
    struct SleepArgs {
        duration: std::time::Duration,
    }

    #[derive(Serialize, Deserialize)]
    enum Routines {
        CheckContext,
        CheckPrivateData(CheckPrivateDataArgs),
        Nop,
        RaiseError,
        SetFlag(SetFlagArgs),
        Sleep(SleepArgs),
    }

    #[async_trait]
    impl Routine<Context> for Routines {
        async fn call(
            &self,
            job: &Job,
            messages_channel: SharedMessageChannel,
            context: Option<Shared<Context>>,
        ) -> Result<Vec<u8>, Error> {
            match self {
                Self::CheckContext => {
                    assert!(context.is_some());
                    assert_eq!(&context.unwrap().lock().unwrap().name, "UNIT_TESTING");

                    Ok(vec![])
                }

                Self::CheckPrivateData(args) => {
                    if args.expect_no_data {
                        assert!(job.private_data::<PrivateData>().is_err());
                    } else {
                        let data = job.private_data::<PrivateData>().unwrap();
                        assert_eq!(data.value, args.value);
                    }

                    Ok(vec![])
                }

                Self::Nop => Ok(vec![]),

                Self::RaiseError => {
                    return Err(Error::Custom("This is a failure".to_string()));
                }

                Self::SetFlag(args) => {
                    let messages_channel = messages_channel.lock().unwrap();

                    set_flag(args.clone());

                    messages_channel
                        .send(Message::Command(Cmd::SetSteps(job.id(), 2)))
                        .unwrap();

                    let json = serde_json::json!({
                        "result": "SET_FLAG_OK",
                    });

                    messages_channel
                        .send(Message::Command(Cmd::SetStep(job.id(), 1)))
                        .unwrap();

                    let bytes = json.to_string().into_bytes();

                    messages_channel
                        .send(Message::Command(Cmd::SetStep(job.id(), 2)))
                        .unwrap();

                    Ok(bytes)
                }

                Self::Sleep(args) => {
                    tokio::time::sleep(args.duration).await;

                    Ok(vec![])
                }
            }
        }
    }

    #[test]
    fn nominal() {
        let mut jq = JobQueueBuilder::<Routines, Context>::new()
            .unwrap()
            .notification_handler(notification_handler)
            .context(Context {
                name: "UNIT_TESTING".to_string(),
            })
            .build();

        reset_flag();

        // Start queue
        jq.start().unwrap();
        assert_eq!(jq.state(), State::Running);

        Runtime::new().unwrap().block_on(async {
            // Create the job and push it
            let routine = Routines::SetFlag(SetFlagArgs { value: true });
            let job = Job::new(routine).unwrap();
            let job_id = job.id();

            jq.enqueue(job).unwrap();
            assert!(jq.remove_job(&job_id).await.is_err());

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            // Verify that job has been processed
            check_flag();
            let bytes = jq.job_result(&job_id).await.unwrap();
            let result: Value = serde_json::from_slice(&bytes).unwrap();
            let status = jq.job_status(&job_id).await.unwrap();
            let progression = jq.job_progression(&job_id).await.unwrap();
            assert_eq!(result["result"], "SET_FLAG_OK");
            assert_eq!(status, Status::Finished(ResultStatus::Success));
            assert_eq!(progression.step, 2);
            assert_eq!(progression.steps, 2);

            // Remove the finished job
            assert!(jq.remove_job(&job_id).await.is_ok());
            assert!(jq.job_status(&job_id).await.is_err());

            // Stop the job queue
            jq.stop().unwrap();
        });

        jq.join().unwrap();
    }

    #[test]
    fn with_thread_pool_size() {
        let mut jq = JobQueueBuilder::<Routines, Context>::new_with_pool_size(1)
            .unwrap()
            .build();

        // Start queue
        jq.start().unwrap();
        assert_eq!(jq.state(), State::Running);

        Runtime::new().unwrap().block_on(async {
            // Create the job and push it
            let job = Job::new(Routines::Nop).unwrap();
            let job_id = job.id();

            jq.enqueue(job).unwrap();

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            // Verify that job has been processed
            let status = jq.job_status(&job_id).await.unwrap();
            assert_eq!(status, Status::Finished(ResultStatus::Success));

            // Stop the job queue
            jq.stop().unwrap();
        });

        jq.join().unwrap();
    }

    #[test]
    fn no_locking() {
        let mut jq = JobQueueBuilder::<Routines, Context>::new_with_pool_size(1)
            .unwrap()
            .build();

        // Start queue
        jq.start().unwrap();
        assert_eq!(jq.state(), State::Running);

        Runtime::new().unwrap().block_on(async {
            // Create the job and push it
            let job = Job::new(Routines::Sleep(SleepArgs {
                duration: tokio::time::Duration::from_millis(100),
            }))
            .unwrap();

            let job_id = job.id();

            jq.enqueue(job).unwrap();

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            // Verify that job has been processed
            let status = jq.job_status(&job_id).await.unwrap();
            assert_eq!(status, Status::Running);

            // Stop the job queue
            jq.stop().unwrap();
        });

        jq.join().unwrap();
    }

    mod context {
        use super::*;

        #[test]
        fn check_context() {
            let mut jq = JobQueueBuilder::<Routines, Context>::new()
                .unwrap()
                .context(Context {
                    name: "UNIT_TESTING".to_string(),
                })
                .build();

            // Start queue
            jq.start().unwrap();
            assert_eq!(jq.state(), State::Running);

            Runtime::new().unwrap().block_on(async {
                // Create the job and push it
                let job = Job::new(Routines::CheckContext).unwrap();
                let job_id = job.id();

                jq.enqueue(job).unwrap();

                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                // Verify that job has been processed
                let status = jq.job_status(&job_id).await.unwrap();
                assert_eq!(status, Status::Finished(ResultStatus::Success));

                // Stop the job queue
                jq.stop().unwrap();
            });

            jq.join().unwrap();
        }
    }

    mod private_data {
        use super::*;

        #[test]
        fn check_private_data() {
            let mut jq = JobQueueBuilder::<Routines, Context>::new().unwrap().build();

            // Start queue
            jq.start().unwrap();
            assert_eq!(jq.state(), State::Running);

            Runtime::new().unwrap().block_on(async {
                let value = 13;

                // Create the job and push it
                let mut job = Job::new(Routines::CheckPrivateData(CheckPrivateDataArgs {
                    value,
                    expect_no_data: false,
                }))
                .unwrap();

                job.set_private_data(PrivateData { value }).unwrap();

                let job_id = job.id();

                jq.enqueue(job).unwrap();

                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                // Verify that job has been processed
                let status = jq.job_status(&job_id).await.unwrap();
                assert_eq!(status, Status::Finished(ResultStatus::Success));

                // Stop the job queue
                jq.stop().unwrap();
            });

            jq.join().unwrap();
        }

        #[test]
        fn check_no_private_data() {
            let mut jq = JobQueueBuilder::<Routines, Context>::new().unwrap().build();

            // Start queue
            jq.start().unwrap();
            assert_eq!(jq.state(), State::Running);

            Runtime::new().unwrap().block_on(async {
                // Create the job and push it
                let job = Job::new(Routines::CheckPrivateData(CheckPrivateDataArgs {
                    value: 0,
                    expect_no_data: true,
                }))
                .unwrap();

                let job_id = job.id();

                jq.enqueue(job).unwrap();

                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                // Verify that job has been processed
                let status = jq.job_status(&job_id).await.unwrap();
                assert_eq!(status, Status::Finished(ResultStatus::Success));

                // Stop the job queue
                jq.stop().unwrap();
            });

            jq.join().unwrap();
        }
    }

    mod expire {
        use super::*;

        #[test]
        fn expire_on_fetch() {
            let mut jq = JobQueueBuilder::<Routines, Context>::new().unwrap().build();

            // Start queue
            jq.start().unwrap();
            assert_eq!(jq.state(), State::Running);

            Runtime::new().unwrap().block_on(async {
                let timeout = std::time::Duration::from_secs(1);

                // Create the job and push it
                let job = Job::new_with_expire(Routines::Nop, ExpirePolicy::OnResultFetch(timeout))
                    .unwrap();

                let job_id = job.id();

                jq.enqueue(job).unwrap();

                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                // Verify that job has been processed
                let status = jq.job_status(&job_id).await.unwrap();
                assert_eq!(status, Status::Finished(ResultStatus::Success));

                // Fetch the result and verify that the job no longer exists after that
                let _ = jq.job_result(&job_id).await.unwrap();
                assert!(jq.job_status(&job_id).await.is_err());

                // Stop the job queue
                jq.stop().unwrap();
            });

            jq.join().unwrap();
        }

        #[test]
        fn expire_on_timeout() {
            let mut jq = JobQueueBuilder::<Routines, Context>::new().unwrap().build();

            // Start queue
            jq.start().unwrap();
            assert_eq!(jq.state(), State::Running);

            Runtime::new().unwrap().block_on(async {
                let seconds = 1;

                // Create the job and push it
                let job = Job::new_with_expire(
                    Routines::Nop,
                    ExpirePolicy::Timeout(std::time::Duration::from_secs(seconds)),
                )
                .unwrap();

                let job_id = job.id();

                jq.enqueue(job).unwrap();

                tokio::time::sleep(tokio::time::Duration::from_secs(seconds / 2)).await;

                // Verify that job is still present
                assert!(jq.job_status(&job_id).await.is_ok());

                // Wait for the timeout to be reached and check again
                tokio::time::sleep(std::time::Duration::from_secs(seconds * 2)).await;
                assert!(jq.job_status(&job_id).await.is_err());

                // Stop the job queue
                jq.stop().unwrap();
            });

            jq.join().unwrap();
        }
    }

    mod concurrent_access {
        use super::*;

        #[test]
        fn can_access() {
            let mut jq = JobQueueBuilder::<Routines, Context>::new_with_pool_size(1)
                .unwrap()
                .build();

            // Start queue
            jq.start().unwrap();
            assert_eq!(jq.state(), State::Running);

            Runtime::new().unwrap().block_on(async {
                // Create the jobs and push them
                let job = Job::new(Routines::Sleep(SleepArgs {
                    duration: tokio::time::Duration::from_secs(1),
                }))
                .unwrap();

                jq.enqueue(job).unwrap();

                let job = Job::new(Routines::Nop).unwrap();
                let job_id = job.id();

                jq.enqueue(job).unwrap();

                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                // Verify that job has been processed
                let status = jq.job_status(&job_id).await.unwrap();
                assert_eq!(status, Status::Finished(ResultStatus::Success));

                // Stop the job queue
                jq.stop().unwrap();
            });

            jq.join().unwrap();
        }
    }

    mod errors {
        use super::*;

        #[test]
        fn not_startable() {
            let mut jq = JobQueueBuilder::<Routines, Context>::new()
                .unwrap()
                .notification_handler(notification_handler)
                .build();

            Runtime::new().unwrap().block_on(async {
                jq.start().unwrap();
                assert!(jq.start().is_err());
            });
        }

        #[test]
        fn not_joinable() {
            let jq = JobQueueBuilder::<Routines, Context>::new()
                .unwrap()
                .notification_handler(notification_handler)
                .build();

            assert!(jq.join().is_err());

            let mut jq = JobQueueBuilder::<Routines, Context>::new()
                .unwrap()
                .notification_handler(notification_handler)
                .build();

            Runtime::new().unwrap().block_on(async {
                jq.start().unwrap();
            });

            assert!(jq.join().is_err());
        }

        #[test]
        fn not_stoppable() {
            let mut jq = JobQueueBuilder::<Routines, Context>::new()
                .unwrap()
                .notification_handler(notification_handler)
                .build();

            Runtime::new().unwrap().block_on(async {
                assert!(jq.stop().is_err());

                jq.start().unwrap();
                jq.stop().unwrap();
                assert!(jq.stop().is_err());
            });
        }

        #[test]
        fn status() {
            let mut jq = JobQueueBuilder::<Routines, Context>::new().unwrap().build();

            // Start queue
            jq.start().unwrap();
            assert_eq!(jq.state(), State::Running);

            Runtime::new().unwrap().block_on(async {
                // Create the job and push it
                let job = Job::new(Routines::RaiseError).unwrap();
                let job_id = job.id();

                jq.enqueue(job).unwrap();

                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                // Verify that job has been processed
                let status = jq.job_status(&job_id).await.unwrap();
                assert_eq!(status, Status::Finished(ResultStatus::Error));

                // Stop the job queue
                jq.stop().unwrap();
            });

            jq.join().unwrap();
        }
    }
}

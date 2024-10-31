# Usage

**Create a handler to catch the notifications**

```rust
fn notification_handler(notif: Notification) {
    match notif { ... }
}
```

**Create a context structure (even if don't need any).**

```rust
struct Context {
    ...
}
```

**Create a routine with its arguments**

```rust
#[derive(Clone, Serialize, Deserialize)]
struct MyRoutineArgs {
    first_arg: String,
}

#[derive(Serialize, Deserialize)]
enum Routines {
    MyRoutine(MyRoutineArgs),
}
```

**Implement the mandatory trait to your routine enum**

```rust
#[async_trait]
impl Routine<Context> for Routines {
    async fn call(
        &self,
        job: &Job,
        messages: SharedMessageChannel,
        context: Option<Shared<Context>>,
    ) -> Result<Vec<u8>, Error> {
        if let Some(context) = context {
            let ctx = context.lock().unwrap();
            // ...
        }

        match self {
            Self::MyRoutine(args) => {
                let messages_channel = messages_channel.lock().unwrap();
                let steps_count = 10;

                // Set the number of steps for this routine
                messages_channel
                    .send(Message::Command(Cmd::SetSteps(job.id(), steps_count)))
                    .unwrap();

                // Run long process
                for idx in 0..steps_count {
                    // ...

                    messages_channel
                        .send(Message::Command(Cmd::SetStep(job.id(), idx)))
                        .unwrap();
                }

                // Build vector of bytes to be returned for storage
                let job_result = [..];

                Ok(job_result)
            }
        }
    }
}
```

**Instantiate the job queue**

```rust
let thread_pool_size = 8;

// NOTE: you can simply call the function `new` to avoid specifying the pool size.
let mut jq = JobQueueBuilder::<Routines>::new_with_pool_size(thread_pool_size)
    .unwrap()
    // optional
    .notification_handler(notification_handler) 
    // optional
    .context(Context{})
    .build();

jq.start().unwrap();

// ...

jq.stop().unwrap();

// wait for the queue to be fully stopped
jq.join().unwrap();
```

**Push a job into the queue**

```rust
// Create the routine with some arguments
let routine = Routines::MyRoutine(MyRoutineArgs { first_arg: 'Hello World'.to_string() });

// Create a job from this routine. By default the job won't expire unless a call to `remove_job` is done.
// If you want an expiration after a period of time you can call `Job::new_with_expire` instead.
let job = Job::new(routine).unwrap();

// Push it
jq.enqueue(job).unwrap();
```

**Get information of a job**

```rust
// Get result (as bytes)
let bytes = jq.job_result(&job_id).await.unwrap();

// Get status of the job
let status = jq.job_status(&job_id).await.unwrap();

// Get progression of the job (if provided by the job itself)
let progression = jq.job_progression(&job_id).await.unwrap();
```

**Attach private data to the job**

You can attach private data to a job. This can be metadata or whatever you need as soon as it's serializable.

```rust
struct PrivateData {
    owner_id: String,
}

// ...

let mut job = Job::new(routine).unwrap();

job.set_private_data(Private{ owner_id: "id-0013".to_string()});

// When the `call` function will be called, the job is provided and you can fetch it like this:

let data: PrivateData = job.private_data().unwrap();
assert_eq!(&data.owner_id, "id-0013");
```

**Override the backend**

A backend is mandatory. By default an implementation based on memory structure is used but you
can override it by creating you're backend structure and implementing the trait `Backend`.
You can, for example, plug it to a REDIS database to store the jobs and their results.

# TODO

- Retry/clean at startup
- Sub jobs ?
- Get list of jobs
- Attach a type or friendly-name to a job

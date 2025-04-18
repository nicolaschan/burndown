use backoff::{ExponentialBackoffBuilder, backoff::Backoff};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::BufRead, process::Stdio, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    process::Command,
    signal::unix::{SignalKind, signal},
    sync::{
        Mutex,
        mpsc::{UnboundedSender, error::TryRecvError, unbounded_channel},
    },
    task::JoinHandle,
    time::Instant,
};

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    cmd: String,

    #[clap(short = 'j', long)]
    concurrency: Option<usize>,

    #[clap(short = 'f', long)]
    checkpoint: Option<String>,

    #[clap(long)]
    max_backoff_ms: Option<u64>,

    #[clap(long)]
    retry_if_suffix: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct CheckpointEntry {
    result: String,
    created_at: String,
}

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
struct CheckpointContents(HashMap<String, CheckpointEntry>);

trait Checkpoint {
    async fn contains_key(&self, key: &str) -> bool;
    async fn get(&self, key: &str) -> Option<CheckpointEntry>;
    async fn insert(&self, key: String, value: CheckpointEntry);
    async fn finish(self);
}

enum CheckpointType {
    Null,
    File(CheckpointFile),
}

struct CheckpointFile {
    sender: UnboundedSender<(String, CheckpointEntry)>,
    contents: Arc<Mutex<CheckpointContents>>,
    handle: JoinHandle<()>,
}

async fn write_contents_if_changed(
    changed: bool,
    file: &mut File,
    contents: &Arc<Mutex<CheckpointContents>>,
) {
    if !changed {
        return;
    }
    let serialized = {
        let contents_guard = contents.lock().await;
        serde_json::to_string_pretty(&*contents_guard).unwrap()
    };
    file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    file.set_len(0).await.unwrap();
    if let Err(e) = file.write_all(serialized.as_bytes()).await {
        eprintln!("Failed to write to checkpoint file: {}", e);
    }
}

impl CheckpointFile {
    async fn new(mut file: File) -> Result<Self> {
        let mut file_contents = String::new();
        file.read_to_string(&mut file_contents).await?;
        let contents: CheckpointContents = serde_json::from_str(&file_contents).unwrap_or_default();
        let contents = Arc::new(Mutex::new(contents));

        let (sender, mut receiver) = unbounded_channel::<(String, CheckpointEntry)>();
        let contents_clone = contents.clone();
        let handle = tokio::spawn(async move {
            let mut changed = false;
            loop {
                match receiver.try_recv() {
                    Ok((key, value)) => {
                        let mut contents_guard = contents_clone.lock().await;
                        contents_guard.0.insert(key.clone(), value);
                        changed = true;
                    }
                    Err(TryRecvError::Empty) => {
                        write_contents_if_changed(changed, &mut file, &contents_clone).await;
                        changed = false;
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => {
                        write_contents_if_changed(changed, &mut file, &contents_clone).await;
                        break;
                    }
                }
            }
        });
        Ok(CheckpointFile {
            sender,
            contents,
            handle,
        })
    }

    async fn new_from_path(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;
        Self::new(file).await
    }
}

impl Checkpoint for CheckpointFile {
    async fn contains_key(&self, key: &str) -> bool {
        let contents = self.contents.lock().await;
        contents.0.contains_key(key)
    }

    async fn get(&self, key: &str) -> Option<CheckpointEntry> {
        let contents = self.contents.lock().await;
        contents.0.get(key).cloned()
    }

    async fn insert(&self, key: String, value: CheckpointEntry) {
        self.sender.send((key, value)).unwrap();
    }

    async fn finish(self) {
        drop(self.sender);
        self.handle.await.unwrap();
    }
}

impl Checkpoint for CheckpointType {
    async fn contains_key(&self, key: &str) -> bool {
        match self {
            CheckpointType::Null => false,
            CheckpointType::File(file) => file.contains_key(key).await,
        }
    }

    async fn get(&self, key: &str) -> Option<CheckpointEntry> {
        match self {
            CheckpointType::Null => None,
            CheckpointType::File(file) => file.get(key).await,
        }
    }

    async fn insert(&self, key: String, value: CheckpointEntry) {
        match self {
            CheckpointType::Null => {}
            CheckpointType::File(file) => file.insert(key, value).await,
        }
    }

    async fn finish(self) {
        match self {
            CheckpointType::Null => {}
            CheckpointType::File(file) => file.finish().await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let concurrency = args.concurrency.unwrap_or(num_cpus::get());
    let backoff = get_backoff(args.max_backoff_ms);
    let checkpoint = get_checkpoint(args.checkpoint).await;

    let multiprogress = MultiProgress::new();

    let stdin = std::io::stdin();
    let lines = stdin.lock().lines().collect::<Vec<_>>();
    let style = ProgressStyle::default_bar()
        .template(
            " {msg:.bold.208} [{bar:25}] {pos}/{len} \x1b[38;5;8m{elapsed}/{duration}\x1b[0m ",
        )
        .unwrap()
        .progress_chars("=> ");
    let progress = multiprogress
        .add(ProgressBar::new(lines.len() as u64))
        .with_style(style);
    progress.set_message("Burning down 🔥");
    progress.enable_steady_tick(Duration::from_millis(1000 / 20));

    let futures = lines.into_iter().map(async |line| {
        let line = line.unwrap();
        if should_use_checkpoint(&checkpoint, &args.retry_if_suffix, &line).await {
            multiprogress
                .println(checkpoint.get(&line).await.unwrap().result)
                .unwrap();
            progress.inc(1);
            return;
        }

        let result = process_with_retry(&args.cmd, &line, &multiprogress, &backoff).await;
        let entry = CheckpointEntry {
            result: result.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
        };
        checkpoint.insert(line, entry).await;
        multiprogress.println(result).unwrap();
        progress.inc(1);
    });

    let futures = stream::iter(futures)
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>();

    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    tokio::select! {
        _ = futures => {}
        _ = sigint.recv() => {}
    }

    checkpoint.finish().await;
    progress.finish_and_clear();
    multiprogress.clear()?;

    Ok(())
}

async fn should_use_checkpoint(
    checkpoint: &CheckpointType,
    retry_if_suffix: &Option<String>,
    line: &str,
) -> bool {
    if !checkpoint.contains_key(line).await {
        return false;
    }

    let value = checkpoint.get(line).await.unwrap();

    if let Some(retry_if_suffix) = retry_if_suffix {
        if value.result.trim().ends_with(retry_if_suffix) {
            return false;
        }
    }

    true
}

fn get_backoff(max_backoff_millis: Option<u64>) -> ExponentialBackoffBuilder {
    let mut backoff = ExponentialBackoffBuilder::new();
    backoff
        .with_initial_interval(Duration::from_millis(100))
        .with_max_interval(Duration::from_secs(30))
        .with_multiplier(2.0)
        .with_randomization_factor(1.0);
    if let Some(max_backoff_millis) = max_backoff_millis {
        backoff.with_max_interval(Duration::from_millis(max_backoff_millis));
    }
    backoff
}

async fn get_checkpoint(checkpoint_file: Option<String>) -> CheckpointType {
    if checkpoint_file.is_none() {
        return CheckpointType::Null;
    }
    let checkpoint_file = CheckpointFile::new_from_path(&checkpoint_file.unwrap()).await;
    CheckpointType::File(checkpoint_file.unwrap())
}

fn build_command(cmd: &str, arg: &str) -> Command {
    let args = shell_words::split(cmd).expect("Failed to split command");
    let mut command = Command::new(args[0].clone());
    command.args(args[1..].iter().map(|s| s.as_str())).arg(arg);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command
}

async fn process_with_retry(
    cmd: &str,
    name: &str,
    multiprogress: &MultiProgress,
    backoff_builder: &ExponentialBackoffBuilder,
) -> String {
    let mut backoff = backoff_builder.build();
    let retry_style = ProgressStyle::default_spinner()
        .template(
            "{spinner} {prefix:.bold.green} \x1b[38;5;8m({pos})\x1b[0m {wide_msg} {elapsed:.8}",
        )
        .unwrap();
    let first_attempt_style = ProgressStyle::default_spinner()
        .template("{spinner} {prefix:.bold.green} {wide_msg} {elapsed:.8}")
        .unwrap();

    let progress = multiprogress.insert(0, ProgressBar::new_spinner());
    progress.set_style(first_attempt_style);
    progress.set_prefix("Running");
    progress.set_message(name.to_string());
    progress.enable_steady_tick(Duration::from_millis(1000 / 20));

    let mut retry = 0;
    loop {
        let command = build_command(cmd, name);
        progress.set_position(retry);

        match run_command(command).await {
            Ok(result) => {
                progress.finish_and_clear();
                return result;
            }
            Err(e) => {
                progress.set_style(retry_style.clone());

                retry += 1;
                let start = Instant::now();
                let next_backoff = backoff.next_backoff().unwrap();

                while start.elapsed() < next_backoff {
                    let wait_remaining = next_backoff.saturating_sub(start.elapsed());
                    progress.set_prefix(format!("\x1b[36mBackoff {}s", wait_remaining.as_secs()));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                let error_string = e.to_string();
                let mut error_lines = error_string.lines();
                let first_line = error_lines.next().unwrap_or_default();
                progress.set_prefix("Running");
                progress.set_message(first_line.to_owned());
            }
        }
    }
}

async fn run_command(mut command: Command) -> Result<String> {
    let mut child = command.spawn().context("Failed to spawn command")?;
    let mut stdout = child
        .stdout
        .take()
        .expect("Child process stdout not captured");
    let mut stderr = child
        .stderr
        .take()
        .expect("Child process stderr not captured");

    let mut buffer = String::new();
    stdout.read_to_string(&mut buffer).await?;
    let status = child.wait().await?;

    if status.success() {
        return Ok(buffer);
    }

    let mut stderr_buffer = String::new();
    stderr.read_to_string(&mut stderr_buffer).await?;
    Err(anyhow!(stderr_buffer))
}

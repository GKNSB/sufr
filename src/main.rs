use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
};

/// Deduplicate huge text/binary files using external sort
#[derive(Parser, Debug)]
struct Args {
    /// Input file
    #[arg(short, long)]
    input: PathBuf,

    /// Output file
    #[arg(short, long)]
    output: PathBuf,

    /// Directory for temporary chunk files
    #[arg(short, long, default_value = "./chunks")]
    temp_dir: PathBuf,

    /// Max lines per chunk
    #[arg(short, long, default_value_t = 1_000_000)]
    chunk_size: usize,
}

/// Represents a line read as raw bytes
type Line = Vec<u8>;

fn main() -> io::Result<()> {
    let args = Args::parse();

    fs::create_dir_all(&args.temp_dir)?;

    // Phase 1: Split into sorted chunks
    let chunk_files = split_into_chunks(&args)?;
    // Phase 2: Merge chunks into final deduped file
    merge_chunks(chunk_files, &args.output)?;

    Ok(())
}

/// Read the input file in chunks, sort, and write temp files
fn split_into_chunks(args: &Args) -> io::Result<Vec<PathBuf>> {
    let mut reader = BufReader::new(File::open(&args.input)?);
    let mut buffer = Vec::with_capacity(args.chunk_size);
    let mut chunk_files = Vec::new();
    let mut line = Vec::new();

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner} Processed lines to chunks... {pos} processed").unwrap(),
    );

    while reader.read_until(b'\n', &mut line)? > 0 {
        buffer.push(line.clone());
        line.clear();
        pb.inc(1);

        if buffer.len() >= args.chunk_size {
            let file = write_sorted_chunk(&args.temp_dir, &mut buffer)?;
            chunk_files.push(file);
        }
    }

    if !buffer.is_empty() {
        let file = write_sorted_chunk(&args.temp_dir, &mut buffer)?;
        chunk_files.push(file);
    }

    pb.finish_with_message("✅ Chunking complete");
    Ok(chunk_files)
}

/// Sort a buffer of raw lines and write to a tempfile
fn write_sorted_chunk(temp_dir: &PathBuf, buffer: &mut Vec<Line>) -> io::Result<PathBuf> {
    buffer.par_sort_unstable_by(|a, b| a.cmp(b));

    let file_path = temp_dir.join(format!("chunk_{}.tmp", uuid::Uuid::new_v4()));
    let mut file = BufWriter::new(File::create(&file_path)?);

    for line in buffer.drain(..) {
        file.write_all(&line)?;
    }

    file.flush()?;
    Ok(file_path)
}

/// Merge sorted chunk files into final deduplicated output
fn merge_chunks(chunk_files: Vec<PathBuf>, output: &PathBuf) -> io::Result<()> {
    let mut readers: Vec<_> = chunk_files
        .iter()
        .map(|p| BufReader::new(File::open(p).unwrap()))
        .collect();

    let mut heap = BinaryHeap::<HeapItem>::new();
    let mut buffers: Vec<Vec<u8>> = vec![Vec::new(); readers.len()];

    for (i, reader) in readers.iter_mut().enumerate() {
        if reader.read_until(b'\n', &mut buffers[i]).unwrap() > 0 {
            heap.push(HeapItem {
                line: buffers[i].clone(),
                index: i,
            });
            buffers[i].clear();
        }
    }

    let mut out = BufWriter::new(File::create(output)?);
    let mut last_written: Option<Line> = None;

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{spinner} Merging lines... {pos} processed").unwrap());

    while let Some(HeapItem { line, index }) = heap.pop() {
        if last_written.as_ref().map_or(true, |prev| *prev != line) {
            out.write_all(&line)?;
            last_written = Some(line.clone());
        }
        pb.inc(1);

        if readers[index].read_until(b'\n', &mut buffers[index]).unwrap() > 0 {
            heap.push(HeapItem {
                line: buffers[index].clone(),
                index,
            });
            buffers[index].clear();
        }
    }

    pb.finish_with_message("✅ Merge complete");

    for f in chunk_files {
        let _ = fs::remove_file(f);
    }

    out.flush()?;
    Ok(())
}

#[derive(Eq, Clone)]
struct HeapItem {
    line: Line,
    index: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other.line.cmp(&self.line)
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.line == other.line
    }
}


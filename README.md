## sufr
Sort and Uniq Files with Rust

Given a huge txt file for say 800+ GB, this little thing cuts it in sorted chunks of let's say 1.000.000 lines. Then uses K-Way and deduplication for merging the chunks into the final sorted and uniq file. Relatively close in speed to sort -u on big files. Also, it uses Rayon for multiprocessing so the more cores you've got, the faster it gets. Finally, it also handles binary lines as each line is treated as bytes instead of text.

```
Usage: sufr [OPTIONS] --input <INPUT> --output <OUTPUT>

Options:
  -i, --input <INPUT>            Input file
  -o, --output <OUTPUT>          Output file
  -t, --temp-dir <TEMP_DIR>      Directory for temporary chunk files [default: ./chunks]
  -c, --chunk-size <CHUNK_SIZE>  Max lines per chunk [default: 1000000]
  -h, --help                     Print help
```

Note: You will have to have created the temp dir for chunk storage because after getting it to work I was too bored to make it create the directory itself.

Example run:
```
sufr --input bigwordlist.txt --output bigwordlist-su.txt --chunk-size 1000000 --temp-dir /mnt/sufrtmpdir
```


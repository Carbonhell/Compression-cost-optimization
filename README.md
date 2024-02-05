# Data compression cost optimization
This repository is an implementation of the [Data Compression Cost Optimization](https://ieeexplore.ieee.org/document/7149296) paper. The project serves as a way to implement and test the framework suggested in the paper.


## Getting started
1. Clone the repository locally.
2. Move the desired document/s to compress in the `data` folder.
3. Run the following command, assuming you have a local Rust toolchain:
    ```sh
    cargo run --release -- --help
    ```
4. The help command shows the various flags that can be used to configure a mixed compression job. You must pass one or more documents (`-d`), along with a time budget (`-b`).
5. In case of large documents, you can estimate algorithm metrics calculation instead of running each possible algorithm. This will speed up the job considerably. Check out the `--estimate` flag help for more info.

## Examples
The related paper uses data from the enwiki repository for evaluation. The exact datasets used in the paper weren't found, but similar results can be achieved with [updated dumps](https://dumps.wikimedia.org/enwiki/20240101/).
To reproduce the claims, download the following documents:
- enwiki-20240101-pagelinks.sql
- enwiki-20240101-langlinks.sql
- enwiki-20240101-pages-meta-history1.xml-p1p844

Unzip them in the data folder, and run the following command:
```sh
cargo run --release -- --budget 3500 --documents enwiki-20240101-pagelinks.sql=gzip,enwiki-20240101-langlinks.sql=gzip,enwiki-20240101-pages-meta-history1.xml-p1p844=bzip2,enwiki-20240101-pages-meta-history1.xml-p1p844=xz2 --estimate --estimate-block-number 10 --estimate-block-ratio 0.001
```

The project will output interesting graphs in the `results` folder showing the lower convex hulls of each workload, along with the lower convex hull for all documents. The same is done for benefits per algorithm for each compression level.

## Decompression
Currently, decompression is out of the scope of this project.

In the future, a decompression method may be added to allow the user to mix completely different algorithms (e.g. Gzip + Bzip2) by adding a small header to the resulting compressed data.

For now, mixes should be made of different levels of the same algorithm, and for this reason popular gzip/bzip2/xz libraries/programs can easily decompress the mixed compressed data. This is possible because the optimal mix will result in two [members (see gzip File Format section, this applies for bzip2 and LZMA in a similar way as well)](https://datatracker.ietf.org/doc/html/rfc1952), one per useful setup.
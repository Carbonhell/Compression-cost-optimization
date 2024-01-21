# Data compression cost optimization
This repository is an implementation of the [Data Compression Cost Optimization](https://ieeexplore.ieee.org/document/7149296) paper. The project serves as a way to test the claims made in the paper.

Data: https://dumps.wikimedia.org/enwiki/20240101/

# Decompression
Currently, decompression is out of the scope of this project.

In the future, a decompression method may be added to allow the user to mix completely different algorithms (e.g. Gzip + Bzip2) by adding a small header to the resulting compressed data.

For now, mixes should be made of different levels of the same algorithm, and for this reason popular gzip/bzip2/xz libraries/programs can easily decompress the mixed compressed data. This is possible because the optimal mix will result in two [members (see gzip File Format section, this applies for bzip2 and LZMA in a similar way as well)](https://datatracker.ietf.org/doc/html/rfc1952), one per useful setup.
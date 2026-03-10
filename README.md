# OSCAR/TEI

According to the [documentation site](https://oscar-project.github.io/documentation/):

> The OSCAR project (Open Super-large Crawled Aggregated coRpus) is an
> Open Source project aiming to provide web-based multilingual
> resources and datasets for Machine Learning (ML) and Artificial
> Intelligence (AI) applications.

This project contains routines for converting the OSCAR corpus
[version 23.01](https://oscar-project.github.io/documentation/versions/oscar-2301/)
to TEI/P5-XML. The converted texts can then be added to the
[DWDS corpus collection](https://www.dwds.de/r/).

## Usage

### Requirements

* [Clojure v1.12](https://clojure.org/)
* [curl CLI](https://curl.se/)

### Setup

Configure your Huma-Num account credentials for authenticated access to OSCAR:

    export OSCAR_USER=…
    export OSCAR_PASSWORD=…

### Downloading the Corpus Sources

    clojure -X zdl.oscar/download

### Converting Downloaded Documents to TEI-P5/XML

    clojure -X zdl.oscar/convert

## License

Copyright 2026– Gregor Middell.

This project is licensed under the GNU General Public License v3.0.

#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

import click
from kg_covid_19 import download as kg_download
from kg_covid_19 import transform as kg_transform
from kg_covid_19.merge_utils.merge_kg import load_and_merge
from kg_covid_19.query import run_query, parse_query_rq, result_dict_to_tsv
from kg_covid_19.transform import DATA_SOURCES


@click.group()
def cli():
    pass


@cli.command()
@click.option("yaml_file", "-y", required=True, default="download.yaml",
              type=click.Path(exists=True))
@click.option("output_dir", "-o", required=True, default="data/raw")
@click.option("ignore_cache", "-i", is_flag=True, default=False,
              help='ignore cache and download files even if they exist [false]')
def download(*args, **kwargs) -> None:
    """Downloads data files from list of URLs (default: download.yaml) into data
    directory (default: data/raw).

    Args:
        yaml_file: Specify the YAML file containing a list of datasets to download.
        output_dir: A string pointing to the directory to download data to.
        ignore_cache: If specified, will ignore existing files and download again.

    Returns:
        None.

    """

    kg_download(*args, **kwargs)

    return None


@cli.command()
@click.option("input_dir", "-i", default="data/raw", type=click.Path(exists=True))
@click.option("output_dir", "-o", default="data/transformed")
@click.option("sources", "-s", default=None, multiple=True,
              type=click.Choice(DATA_SOURCES.keys()))
def transform(*args, **kwargs) -> None:
    """Calls scripts in kg_covid_19/transform/[source name]/ to transform each source
    into nodes and edges.

    Args:
        input_dir: A string pointing to the directory to import data from.
        output_dir: A string pointing to the directory to output data to.
        sources: A list of sources to transform.

    Returns:
        None.

    """

    # call transform script for each source
    print(f'Using input from `{kwargs["input_dir"]}`')
    print(f'Storing output to `{kwargs["output_dir"]}`')
    kg_transform(*args, **kwargs)

    return None


@cli.command()
@click.option('yaml', '-y', default="merge.yaml", type=click.Path(exists=True))
@click.option('processes', '-p', default=1, type=int)
def merge(yaml: str, processes: int) -> None:
    """Use KGX to load subgraphs to create a merged graph.

    Args:
        yaml: A string pointing to a KGX compatible config YAML.
        processes: Number of processes to use.

    Returns:
        None.

    """

    load_and_merge(yaml, processes)



if __name__ == "__main__":
    cli()

"""Entry point to run ETL."""
# pylint: disable=no-value-for-parameter
import logging
import logging.config
import sys

import click

import bonobo
from bi.pipeline.pipeline_generator import PipelineGenerator


@click.command()
@click.option("-f", "--file", help="Your yaml pipeline file.", type=str, required=True)
def main(file):
    """Main function to run ETL."""
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    pyg = PipelineGenerator(file)

    graph = pyg.construct_graph()
    bonobo.run(graph)
    logger.info("End of bonobo graph execution")


if __name__ == "__main__":
    main()

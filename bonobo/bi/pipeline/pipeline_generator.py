"""Pipeline Generator class to construct bonobo graph."""
import logging
import typing as tp

import bonobo
import yaml
from bi.data_io.csv_loader import CSVLoader
from bi.data_io.json_extractor import JsonExtractor
from bi.transformation.filterer import Filterer
from bi.transformation.selector import Selector
from bi.transformation.renamer import Rename
from bi.transformation.aggregator import Aggregator

logger = logging.getLogger(__name__)

CLASS = {
    # DATA I/O
    "JsonExtractor": JsonExtractor,
    "CSVLoader": CSVLoader,
    # TRANSFORMATIONS
    "Filterer": Filterer,
    "Selector": Selector,
    "Rename": Rename,
    "Aggregator": Aggregator,
}


class PipelineGenerator:
    """Class used to generate pipeline."""

    def __init__(self, file: str):
        """Init pipeline yaml generator.

        Args:
            file: Path to the YAML configuration file which contains the
                  pipeline configuration.
        """
        self._file = file

    def _load_datas(self) -> tp.Dict[str, dict]:
        """Extract data from a yaml file.

        Returns:
            Dict with all data configuration.

        Raises:
            YAMLError: If yaml loading fail.
        """
        with open(self._file, "r") as stream:
            try:
                load: tp.Dict[str, dict] = yaml.safe_load(stream)
                logger.info("YAML imported")
                return load
            except yaml.YAMLError as exc:
                logger.debug("YAML import error : %s", exc)
                raise

    def construct_graph(self) -> bonobo.Graph:
        """Construct bonobo graph.

        Returns:
            Bonobo graph ready to be executed.
        """
        nodes = self._load_datas()

        instance_dict: tp.Dict[str, tp.Any] = {}

        for k, v in nodes.items():
            if "in" in v:
                input_nodes = len(v["in"])
                if "args" not in v or not v["args"]:
                    v["args"] = {"nb_input_nodes": input_nodes}
                else:
                    v["args"]["nb_input_nodes"] = len(v["in"])

            instance_dict[k] = CLASS[v["class"]](**v["args"])

        graph = bonobo.Graph()

        chain_dict = self._compute_chain_dict(nodes)

        for k, v in chain_dict.items():
            if not v:
                graph.add_chain(instance_dict[k], _name=k)
            else:
                graph.add_node(instance_dict[k], _name=k)
                for node in v:
                    graph.add_chain(_input=node, _output=k)

        return graph

    def _compute_chain_dict(self, configuration: dict) -> dict:
        """Construct dict with input/outputs present in configuration dict

        Args:
            configuration: Dict with nodes and args to construct bonobo graph.

        Returns:
            Dict with keys (in) and values (outs) of each graph nodes.

        """
        chain_dict = {}

        for k, v in configuration.items():
            input_val = v.get("in", [])
            chain_dict[k] = input_val

        return chain_dict

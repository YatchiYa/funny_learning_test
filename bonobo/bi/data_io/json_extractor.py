# pylint: disable=too-few-public-methods
"""JsonExtractor from json documents."""

import json
import logging
import logging.config
import typing as tp

from bi.pipeline_node import PipelineNode
from bi.utils.return_flag import STOP_ITERATION, isstopiteration

logger = logging.getLogger(__name__)


class JsonExtractor(PipelineNode):
    """Extract documents from json files in a directory.

    Attributes:
        dir (str): directory containing all the json files.
        limit (int): limit the number of documents.
    """

    def __init__(self, *args, **kwargs) -> None:
        """Initiate a JsonExtractor block."""
        super().__init__(*args, **kwargs)

        self._file: str = kwargs.get("file", "")
        self._limit: int = kwargs.get("limit", 0)
        self._count_stop_iteration = 0

    def __call__(
        self, document: tp.Optional[tp.Dict[str, tp.Any]] = None
    ) -> tp.Optional[tp.Generator[dict, None, None]]:
        """Create the JsonExtractor to get documents.

        Arguments:
            document (dict): input dictionnary, if it exists.

        Yields:
            new_document (dict): output from json file.

        Example:
            args:
                dir: "/Users/name/jsonstorage/set_1/"
                limit: 5
        """
        if document is not None and isstopiteration(document):
            self._count_stop_iteration += 1
            if self._count_stop_iteration == self._nb_input_nodes:
                yield STOP_ITERATION
        else:
            with open(self._file, "r") as file:
                data = json.load(file)
            for yield_count, obj in enumerate(data):
                if self._limit == 0 or yield_count < self._limit:
                    yield dict(obj)
                else:
                    break

            # After you've yielded all documents, yield the STOP_ITERATION
            if self._nb_input_nodes == 0:
                yield STOP_ITERATION

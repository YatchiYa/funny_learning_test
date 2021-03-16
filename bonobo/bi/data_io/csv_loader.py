"""Load data into CSV file."""
import logging
import typing as tp
from copy import deepcopy

import pandas as pd

from bi.pipeline_node import PipelineNode
from bi.utils.return_flag import isstopiteration

logger = logging.getLogger()


class CSVLoader(PipelineNode):
    """Format documents and output to XLS file.

    Attributes:
        file (str): directory and file name.
        columns (list): select columns to include in csv file.
    """

    def __init__(self, *args, **kwargs) -> None:
        """Initiate XlsLoader block and output a file."""
        super().__init__(*args, **kwargs)
        self._file: str = kwargs.get("file", "default")
        self._file += ".csv"
        self._count_stop_iteration = 0
        self._buffer: list = []

    def __call__(self, document: tp.Optional[dict]) -> None:
        """Insert documents in a Pandas Dataframe and output it as an csv file.

        Arguments:
            document (dict): input dictionnary.
        """
        if document is None:
            return
        if isstopiteration(document):
            df = pd.json_normalize(self._buffer)
            df.to_csv(
                self._file,
                index=False,
            )
            self._buffer.clear()
            if isstopiteration(document):
                logging.info("Wrote result to csv file.")
        else:
            self._buffer.append(deepcopy(document))
        return

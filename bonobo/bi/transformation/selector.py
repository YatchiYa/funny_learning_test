# pylint: disable=too-few-public-methods
"""Selector block to select the fields to yield."""

import typing as tp
from copy import deepcopy

from bi.pipeline_node import PipelineNode

from bi.utils.return_flag import (  # type: ignore # noqa: E501; type: ignore # noqa: E501
    STOP_ITERATION,
    isstopiteration,
)


class Selector(PipelineNode):
    """Selector block to select the fields to yield.

    Attributes:
        keys (list): list of keys to select
        drop (bool): drop the keys instead
    """

    def __init__(self, *args, **kwargs):
        """Initiate Selector block."""
        super().__init__(*args, **kwargs)
        self._count_stop_iteration = 0
        self._keys = kwargs.get("keys", [])
        self._drop = kwargs.get("drop", False)

    def __call__(
        self, document: tp.Optional[dict]
    ) -> tp.Optional[tp.Generator[dict, None, None]]:
        """Select fields in keys to reduce size of document.

        Arguments:
            document: document you want to change.

        Yields:
            Generator: Document with the updated values.

        Example:
            args:
                keys: ['token', 'unix']

            input -> {'token': 'AAA', 'extra':'ordinary', 'unix': 523}

            output -> {'token': 'AAA', 'unix': 523}
        """
        if document is None:
            return

        if isstopiteration(document):
            self._count_stop_iteration += 1
            if self._count_stop_iteration == self._nb_input_nodes:
                yield STOP_ITERATION
        else:
            if self._drop:
                new_doc = deepcopy(document)
                for key in self._keys:
                    try:
                        new_doc.pop(key)
                    except KeyError:
                        continue
            else:
                new_doc = dict()
                for key in self._keys:
                    try:
                        new_doc[key] = document[key]
                    except KeyError:
                        continue
            yield new_doc

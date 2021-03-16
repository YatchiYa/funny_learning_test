# pylint: disable=too-few-public-methods
"""Superclass PipelineNode for all transformations class"""

import typing as tp
from abc import abstractmethod


class PipelineNode:
    """Class used to generate pipeline"""

    def __init__(self, *args, **kwargs):
        """Init node method.

        Args:
            args: not used.
            kwargs:
                nb_input_nodes: .
        """
        self._nb_input_nodes = kwargs.get("nb_input_nodes", 0)
        self._count_stop_iteration = 0
        self._debug_count = 0

    @abstractmethod
    def __call__(
        self, document: tp.Optional[dict]
    ) -> tp.Optional[tp.Generator[dict, None, None]]:  # noqa E501
        """Take a document and do transformation.

        Returns:
            Transformed document(s).
        """

# pylint: disable=too-few-public-methods
"""Filterer block to filter documents based on rules."""

import logging
import logging.config
import typing as tp
from copy import deepcopy

from bi.pipeline_node import PipelineNode
from bi.utils.fun_rules import rule_equal
from bi.utils.return_flag import STOP_ITERATION  # type: ignore # noqa: E501
from bi.utils.return_flag import isstopiteration

logger = logging.getLogger(__name__)

FUN_RULES = {
    "rule_equal": rule_equal,
}


class Filterer(PipelineNode):
    """Filterer block to filter documents based on rules.

    Attributes:
        functions (list): functions used to filter documents.
    """

    def __init__(self, *args, **kwargs):
        """Initiate Filterer block."""
        super().__init__(*args, **kwargs)
        self._count_stop_iteration = 0
        self._functions: tp.List[tp.Dict[str, tp.Any]] = kwargs.get("functions", None)

    def __call__(
        self, document: tp.Optional[dict]
    ) -> tp.Optional[tp.Generator[dict, None, None]]:
        """Only returns documents that match conditions set by functions.

        If you set multiple conditions, all of them must be met.
        Cond1 AND cond2 AND cond3.

        Arguments:
            document: doc you want to apply filter on.

        Yields:
            Generator: Documents.

        Example:
            args:
                functions:
                    - fun: 'rule_contains'
                      kwargs:
                        space: 'words'
                        value: 'smile'
                    - fun: 'rule_equal'
                      kwargs:
                        key1: 'token'
                        value1: 'A'
                        _not: 1

            input -> {'token': 'A', 'words': ['a', 'wild', 'cariboo']}
                     {'token': 'B', 'words': ["Pete's", 'devilish', 'smile']}

            output -> {'token': 'B', 'words': ["Pete's", 'devilish', 'smile']}
        """
        if document is None:
            return

        if isstopiteration(document):
            self._count_stop_iteration += 1
            if self._count_stop_iteration == self._nb_input_nodes:
                yield STOP_ITERATION
        else:
            new_doc = deepcopy(document)
            if self._functions:
                self._must_yield = True
                for fun_dict in self._functions:
                    if any(k not in fun_dict for k in ["fun", "kwargs"]):
                        return
                    else:
                        try:
                            f = FUN_RULES[fun_dict["fun"]]
                        except ImportError as err:
                            logger.error("Function import error : %s", err)
                        if not f(new_doc, **fun_dict["kwargs"]):
                            self._must_yield = False
                if self._must_yield:
                    yield new_doc

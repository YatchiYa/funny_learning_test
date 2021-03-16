# pylint: disable=too-few-public-methods
"""Aggregator block to select the fields to yield."""

import typing as tp
from copy import deepcopy

from bi.pipeline_node import PipelineNode

from bi.utils.return_flag import (  # type: ignore # noqa: E501; type: ignore # noqa: E501
    STOP_ITERATION,
    isstopiteration,
)


class Aggregator(PipelineNode):
    """Aggregate fields or column which has the same key

    Arguments:
        document: document you want to change.

    Yields:
        Generator: Document with the updated values.

    Example:
            input: [
                {token: A, name: Alice},
                {token: B, name: Bob},
                {token: A, name: Charlie}
            ]
            output: (aggréger sur token le champ name) [
                {token: A, name: [Alice, Charlie]},
                {token: B, name: [Bob]},
            ]
    """
    def __init__(self, *args, **kwargs):
        """Initiate Aggregator block."""
        super().__init__(*args, **kwargs)
        self._count_stop_iteration = 0
        self._keys = kwargs.get("keys", [])
        # set the list of the documents 
        # so we can compare between their key to aggregate thos who have same key
        self._data = []

    def __call__(
        self, document: tp.Optional[dict]
    ) -> tp.Optional[tp.Generator[dict, None, None]]:

        if document is None:
            return

        if isstopiteration(document):
            self._count_stop_iteration += 1
            if self._count_stop_iteration == self._nb_input_nodes:
                # yield tje final document befor stop iteration
                for element in self._data:
                    yield element
                yield STOP_ITERATION
        else:
            #define the new doc that will contain the result 
            new_doc = dict() 
            for (key, value) in document.items():
                if (key == self._keys[1]):
                    data = []
                    data.append(value)
                    new_doc[key] = data
                else:
                    new_doc[key] = value 
                    
            # verifie if's already exists in our data
            check = False
            for element in self._data:
                if (element[self._keys[0]] == new_doc[self._keys[0]]):
                    element[self._keys[1]].append(new_doc[self._keys[1]][0])
                    check = True
            if (check == False):
                self._data.append(new_doc)
            
                
            

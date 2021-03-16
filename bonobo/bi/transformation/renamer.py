# pylint: disable=too-few-public-methods
"""Rename block to select the fields to yield."""

import typing as tp
from copy import deepcopy

from bi.pipeline_node import PipelineNode

from bi.utils.return_flag import (  # type: ignore # noqa: E501; type: ignore # noqa: E501
    STOP_ITERATION,
    isstopiteration,
)


class Rename(PipelineNode):
    """Rename field or column

    Arguments:
        document: document you want to change.

    Yields:
        Generator: Document with the updated values.

    Example:
        input: [
            {token: A, name: Alice},
            {token: B, name: Bob},
            {token: C}
        ]
        output: (rename champ name to nom)
        garder ancien champ: [
            {token: A, name: Alice, nom: Alice},
            {token: B, name: Bob, nom: Bob},
            {token: C}
        ]
        supprimer ancien champ: [
            {token: A, nom: Alice},
            {token: B, nom: Bob},
            {token: C}
        ]
        valeur par default égal à blank: [
            {token: A, name: Alice, nom: Alice},
            {token: B, name: Bob, nom: Bob},
            {token: C, nom: blank}
        ]
    """
    def __init__(self, *args, **kwargs):
        """Initiate Rename block."""
        super().__init__(*args, **kwargs)
        self._count_stop_iteration = 0
        self._keys = kwargs.get("keys", [])
        self._mode: str = kwargs.get("mode", "")

    def __call__(
        self, document: tp.Optional[dict]
    ) -> tp.Optional[tp.Generator[dict, None, None]]:
    
        if document is None:
            return

        if isstopiteration(document):
            self._count_stop_iteration += 1
            if self._count_stop_iteration == self._nb_input_nodes:
                yield STOP_ITERATION
        else:
            # for the mode definition: 
            # 1 : saving the name and add the new champs nom    
            # 2 : delete the name and add the new champs nom    
            # 3 : set the default value blank  
            if (self._mode == "1"):
                yield self._saving_champs(document)
            elif (self._mode == "2"):
                yield self._without_saving_champs(document)
            elif (self._mode == "3"):
                yield self._with__default_champs(document)
            else:
                # this one just for default, knowing that one of the 3 mode should be active
                yield self._with__default_champs(document)


    # define the different function
    # with saving the previous champs
    def _saving_champs(self, document):
        new_doc = dict()
        for (key, value) in document.items():
            new_doc[key] = value
            if (key == self._keys[0]):
                new_doc[self._keys[1]] = value
        return new_doc  

    # whithout saving the previous champs
    def _without_saving_champs(self, document):
        new_doc = dict()
        for (key, value) in document.items():
            if (key == self._keys[0]):
                new_doc[self._keys[1]] = value
            elif (key != self._keys[0]):
                new_doc[key] = value
        return new_doc
        
    # with the blank default value
    def _with__default_champs(self, document):
        _name_found = False
        new_doc = dict()
        for (key, value) in document.items():
            new_doc[key] = value
            if (key == self._keys[0]):
                _name_found = True
                new_doc[self._keys[1]] = value
        if (_name_found == False):
            new_doc[self._keys[1]] = "blank"
            new_doc[self._keys[0]] = ""
        return new_doc
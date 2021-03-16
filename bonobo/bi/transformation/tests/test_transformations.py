"""Unit testing for transformations functions."""
import json
from ast import literal_eval

from bi.utils.return_flag import STOP_ITERATION  # type: ignore # noqa: E501
from bi.transformation.filterer import Filterer
from bi.transformation.selector import Selector

CLASS = {
    # TRANSFORMATIONS
    "Filterer": Filterer,
    "Selector": Selector,
}

def test_from_json(json_file: str):
    """Execute test from json configuration.

    Args:
        json_file: file with config, input and output for test
    """
    with open(json_file, "r") as file:
        data = json.load(file)
        run_test(data)


def run_test(data: dict):
    """Generic function to run unit test.

    Args:
        data: information for config, input and output for test
    """
    config_args = data["config"].get("args", [])
    config_kwargs = data["config"].get("kwargs", {})
    num_input = data["config"].get("num_input", 1)
    config_kwargs["nb_input_nodes"] = num_input

    block = CLASS[data["config"]["class"]](*config_args, **config_kwargs)

    for k, v in data["datas"].items():
        if isinstance(v["inputs"], str):
            input_data = literal_eval(v["inputs"])
        else:
            input_data = v["inputs"]
        if isinstance(v["outputs"], str):
            expected_output_data = literal_eval(v["outputs"])
        else:
            expected_output_data = v["outputs"]

        # Put all docs in after transformation in actual_output
        input_all = [doc for doc in input_data] + [
            STOP_ITERATION  # type: ignore
        ] * num_input
        actual_output = []
        for in_doc in input_all:
            actual_output.extend(list(block(in_doc)))

        # Check if all docs are there
        assert actual_output, "Output should not be empty"
        assert (
            len(actual_output) == len(expected_output_data) + 1
        ), (
            "Length of actual and expected outputs should be the same."
            "(+ STOP_ITERATION item)"
        )
        assert (
            actual_output[-1] == STOP_ITERATION
        ), "Output last item should be a STOP_ITERATION."

        for index, output_doc in enumerate(expected_output_data):
            assert output_doc in actual_output, (
                    "Any item in actual output should be in expected output."
                    f"Expected doc: {output_doc}, "
                    f"not found in actual output: {actual_output}."
            )
            assert output_doc == actual_output[index], (
                "Actual output item should be at expected index"
                f"Expected doc: {output_doc} at index: {index}, "
                f"found at incorrect index in yielded: {actual_output}."
            )

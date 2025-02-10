from dm8gen.Helper.Helper import Hasher, Helper

import pytest
from pytest_cases import parametrize_with_cases
from .test_020_helper_cases import (
    HashCases,
    AlgorithmCases,
    UuidCases,
    SolutionFilePathCases,
)


@parametrize_with_cases("algorithm", cases=AlgorithmCases, glob="*_valid")
@parametrize_with_cases("input", cases=HashCases, glob="*_valid")
def test_hasher_hash(input, algorithm):
    input, checksum = input
    hasher = Hasher(algorithm)
    hash = hasher.hash(input)

    assert hash.hexdigest() == checksum


@parametrize_with_cases("algorithm", cases=AlgorithmCases, glob="*_valid")
@parametrize_with_cases("input", cases=UuidCases, glob="*_valid")
def test_hasher_create_uuid(input, algorithm):
    input, checksum = input
    hasher = Hasher(algorithm)
    uuid = hasher.create_uuid(input)

    assert str(uuid) == checksum


@parametrize_with_cases("algorithm", cases=AlgorithmCases, glob="*_invalid")
def test_hasher(algorithm):
    with pytest.raises(Hasher.UnknownAlgorithmExpcetion):
        Hasher(algorithm)


@parametrize_with_cases("input", cases=SolutionFilePathCases, glob="*_valid")
def test_get_solution_path(input):
    solution_file_path, check_value = input
    solution_path = Helper.get_parent_path(solution_file_path)

    assert solution_path == check_value

from pytest_cases import parametrize
from pathlib import Path


class AlgorithmCases:
    @parametrize("algorithm", ["SHA256"])
    def case_algorithm_valid(self, algorithm):
        return algorithm

    @parametrize("algorithm", ["MD5"])
    def case_algorithm_invalid(self, algorithm):
        return algorithm


class HashCases:
    @parametrize(
        "input",
        [
            # format: ("input", "64-byte hash")
            (
                "test",
                "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
            ),
            (
                "test2",
                "60303ae22b998861bce3b28f33eec1be758a213c86c93c076dbe9f558c11c752",
            ),
        ],
    )
    def case_hashes_valid(self, input):
        return input


class UuidCases:
    @parametrize(
        "input",
        [
            # format: ("input", "uuid")
            ("test", "98d88476-92ea-c5d1-ab41-2082d561bf00"),
            ("test2", "633e2986-beb8-3ecb-7823-8c306b9581c5"),
        ],
    )
    def case_uuid_valid(self, input):
        return input


class SolutionFilePathCases:
    @parametrize(
        "input",
        [
            # format: ("solution_file", "solution_folder")
            (r"C:\test\test\solution_file.dm8s", r"C:\test\test"),
            ("/home/user/solution/solution_file.dm8s", "/home/user/solution"),
            (r".\solution\solution_file.dm8s", r".\solution"),
            ("../solution/solution_file.dm8s", "../solution"),
        ],
    )
    def case_solution_folder_valid(self, input):
        file, folder = input
        return (Path(file), Path(folder))

from enum import Enum
from DefaultValue import DefaultValue
import logging


class Result(Enum):
    NONE = 1
    OK = 2
    NOT_EQUAL = 3
    UNEXPECTED_MODIFICATION = 4
    VALIDATE_MANUALLY = 5
    UNMODIFICATION = 6


class KeyResult:
    def __init__(self, expected, actual=None, result=Result.NONE):
        self._expected = expected
        self._actual = actual
        self._result: Result = result

    @property
    def expected(self):
        return self._expected

    @expected.setter
    def expected(self, value):
        self._expected = value

    @property
    def actual(self):
        return self._actual

    @actual.setter
    def actual(self, value):
        self._actual = value

    @property
    def result(self):
        return self._result

    def validate(self, actual_to_validate):
        self._actual = actual_to_validate
        expected_val = str(self.expected)
        logging.info(f"Validating {self._actual} vs {expected_val}")
        if type(self._actual) is tuple:
            actual = ','.join(map(str, self._actual))
        elif type(self.actual) is not list and type(self.actual) is not tuple:
            expected_val = expected_val.lower()
            if type(self.actual) is bool:
                actual = str(self.actual).lower()
            elif type(self.actual) is float:
                if self.actual.is_integer():
                    actual = str(int(self.actual))
                else:
                    actual = str(self.actual)
            else:  # type(actual) is int
                actual = str(self.actual).lower()
        if expected_val.__contains__('<'):
            self._result = Result.VALIDATE_MANUALLY
        elif expected_val != actual:
            if actual != DefaultValue.DEFAULT_VALUE_INI and actual != DefaultValue.DEFAULT_VALUE_JSON:
                self._result = Result.NOT_EQUAL
            else:  # actual == default_value
                self._result = Result.UNMODIFICATION
        else:  # expected_val == actual
            self._result = Result.OK

"""Provides encapsulation for CSV parsing."""
# pylint: disable=consider-using-with
import csv


class CSVReader:
    """Allows for CSV parsing."""

    def __init__(self, filepath):
        """Open given csv filepath and initialize reader for it."""
        self._file = open(filepath, "r", encoding="utf-8")
        self._csv_reader = csv.reader(self._file, delimiter=',')
        next(self._csv_reader)

    def next_line(self):
        """Return next line in csv."""
        return next(self._csv_reader)

    def __del__(self):
        """Close open file if there is one."""
        try:
            self._file.close()
        except AttributeError:
            pass
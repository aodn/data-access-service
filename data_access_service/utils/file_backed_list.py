import json
import tempfile
import os
import gzip

from typing import Dict, Iterator, Union, List
from contextlib import contextmanager

from data_access_service import init_log, Config


class FileBackedList(list):
    """A list subclass that stores JSON records in an uncompressed JSON Lines file with slicing support."""

    def __init__(self, file_path: str = None):
        super().__init__()
        if file_path is None:
            self.temp_file = tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8', suffix='.jsonl', delete=False)
            self.file_path = self.temp_file.name
        else:
            self.file_path = file_path
            self.temp_file = None
        self._file = open(self.file_path, 'w+', encoding='utf-8')
        self._length = 0
        self._closed = False
        self._line_positions = []  # Store byte offsets for each line

    def append(self, item: Dict):
        """Append a JSON record to the file."""
        if self._closed:
            raise ValueError("Cannot append to closed FileBackedList")
        if not isinstance(item, dict):
            raise TypeError("Item must be a dictionary")
        # Record the current file position
        pos = self._file.tell()
        json_str = json.dumps(item) + '\n'
        self._file.write(json_str)
        self._file.flush()
        self._line_positions.append(pos)
        self._length += 1

    def __iter__(self) -> Iterator[Dict]:
        """Iterate over records by reading the file lazily."""
        self.close()
        with open(self.file_path, 'r', encoding='utf-8') as f:
            for line in f:
                yield json.loads(line.strip())

    def __len__(self) -> int:
        """Return the number of records."""
        return self._length

    def __getitem__(self, key: Union[int, slice]) -> Union[Dict, List[Dict]]:
        """
        Retrieve a single record or a slice of records from the JSON Lines file using seek.

        Args:
            key: Integer index or slice object (e.g., slice(i, i + chunk_size)).

        Returns:
            Dict for single index, List[Dict] for slice.

        Raises:
            IndexError: If index is out of range.
            ValueError: If slice range is invalid.
            TypeError: If key is not an int or slice.
        """
        self.close()

        if isinstance(key, int):
            # Handle single index (e.g., lst[i])
            if key < 0:
                key += self._length
            if key < 0 or key >= self._length:
                raise IndexError(f"Index {key} out of range for length {self._length}")

            with open(self.file_path, 'r', encoding='utf-8') as f:
                f.seek(self._line_positions[key])
                line = f.readline()
                return json.loads(line.strip())

        elif isinstance(key, slice):
            # Handle slice (e.g., lst[i : i + chunk_size])
            start, stop, step = key.indices(self._length)
            if step != 1:
                raise NotImplementedError("Slicing with step != 1 not supported")
            if start < 0 or stop < 0 or start > stop or start >= self._length:
                raise ValueError(f"Invalid slice range: [{start}:{stop}]")

            result = []
            with open(self.file_path, 'r', encoding='utf-8') as f:
                f.seek(self._line_positions[start])
                # Read up to stop - start lines
                for i in range(start, min(stop, self._length)):
                    line = f.readline()
                    if not line:
                        break
                    result.append(json.loads(line.strip()))
            return result

        else:
            raise TypeError("Index must be an integer or slice")

    def close(self):
        """Close the file."""
        if not self._closed:
            self._file.close()
            self._closed = True

    def __del__(self):
        """Clean up the temporary file."""
        try:
            self.close()
            if self.temp_file and os.path.exists(self.file_path):
                os.unlink(self.file_path)
        except Exception:
            pass

    @contextmanager
    def cleanup(self):
        """Context manager to ensure file cleanup."""
        try:
            yield self
        finally:
            self.__del__()

    def __setitem__(self, index, value):
        raise NotImplementedError("Item assignment not supported")

    def __delitem__(self, index):
        raise NotImplementedError("Item deletion not supported")

    def insert(self, index, item):
        raise NotImplementedError("Insert not supported; use append")

    def pop(self, index=-1):
        raise NotImplementedError("Pop not supported")

    def remove(self, item):
        raise NotImplementedError("Remove not supported")
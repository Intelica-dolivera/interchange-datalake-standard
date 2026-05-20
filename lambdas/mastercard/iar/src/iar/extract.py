import io
from pathlib import Path

def unblock_file(fileobj: io.BytesIO) -> bytes:
    fileobj.seek(0)
    chunk_array = bytearray()

    while True:
        chunk = fileobj.read(1012)
        chunk_array.extend(chunk)

        block_separator = fileobj.read(2)

        if block_separator not in [
            b"",
            b"\x00\x00",
            b"\x20\x20",
            b"\x40\x40",
        ]:
            fileobj.seek(fileobj.tell() - 2)

        if len(chunk) < 1012:
            break

    return bytes(chunk_array)


def extract_iar_file(path_to_file: str | Path, blocked: bool = True) -> io.BytesIO:
    with open(path_to_file, "rb") as f:
        bufferfile = io.BytesIO(f.read())

    if blocked:
        unblocked = unblock_file(bufferfile)
        return io.BytesIO(unblocked)

    return bufferfile

def extract_iar_bytes(
    file_bytes: bytes,
    blocked: bool,
):
    if blocked:
        file_bytes = unblock_file(
            io.BytesIO(file_bytes)
        )

    return io.BytesIO(file_bytes)
import email
import typing
import io
import functools
import json
import pandas as pd
import s3fs

from pathlib import Path


def _delete_earliest_files_if_size_overflowed(
    directory: Path, num_files: int = 10, thresh_size: int = 2 * 1024 * 1024 * 1024
) -> None:
    """
    - checks the total size of the input directory and if it is greater than thresh_size then
    deletea the earliest num_files files from the directory
    """
    if not directory.is_dir():
        raise ValueError("Provided path is not a directory.")

    files = [(f, f.stat().st_mtime) for f in directory.iterdir() if f.is_file()]
    total_size = sum(f.stat().st_size for f, _ in files)

    if total_size > thresh_size:
        print(
            f"deleting the earliest 10 files because cache directory size ({total_size/(1024**3)}) GBs is more than {thresh_size/(1024**3)} GBs"
        )
        files.sort(key=lambda x: x[1])  # Sort files by modification time
        for file in files[:num_files]:
            file.unlink()


def s3_read_bytes(
    key: str,
    cache_path: typing.Union[Path, None] = "/home/ec2-user/SageMaker/.s3_cache",
    override_cache: bool = False,
) -> bytes:
    """
    - reads bytes from s3 and keeps a cache to store latest files locally
    """

    if isinstance(cache_path, str):
        cache_path = Path(cache_path)

    save_file_path = cache_path.joinpath(key)
    save_file_path.parent.mkdir(parents=True, exist_ok=True)

    if override_cache and save_file_path.is_file():
        print("overriding cache")
        save_file_path.unlink()

    if save_file_path.is_file():
        print(f"loading {key} from cache")
        with open(save_file_path.as_posix(), "rb") as cache_file:
            result = cache_file.read()
        return result

    print(f"file not found in cache, reading {key} from s3 and storing in cache")
    fs = s3fs.S3FileSystem()
    with fs.open(key, "rb") as f:
        result = f.read()

    _delete_earliest_files_if_size_overflowed(save_file_path.parent)
    with open(save_file_path.as_posix(), "wb") as cache_file:
        cache_file.write(result)

    return result


@functools.lru_cache(100)
def s3_read_json(key: str, **read_bytes_kwargs) -> dict:
    """
    - reads json from the input s3 key
    """
    bytes_data = s3_read_bytes(key, **read_bytes_kwargs)
    return json.loads(bytes_data.decode("utf-8"))


@functools.lru_cache(100)
def s3_read_csv(key: str, **read_bytes_kwargs) -> pd.DataFrame:
    """
    - reads csv from the input s3_key
    """
    bytes_data = s3_read_bytes(key, **read_bytes_kwargs)
    df = pd.read_csv(io.BytesIO(bytes_data))
    return df


@functools.lru_cache(100)
def s3_read_email(key: str, **read_bytes_kwargs) -> email.message.Message:
    """
    @args:
        key: assuming the a typical s3_key looks as s3://bucket/path/to/file, this function expects
    the input key to be "bucket/path/to/file"
    """
    bytes_data = s3_read_bytes(key, **read_bytes_kwargs)
    email_obj = email.message_from_bytes(bytes_data)
    return email_obj


def s3_write_csv(key: str, df: pd.DataFrame) -> None:
    """
    - writes the input df to the input key on s3
    """
    fs = s3fs.S3FileSystem()
    with fs.open(key, "w") as f:
        df.to_csv(f, index=False)


def s3_list_keys(parent_key: str) -> list:
    """
    - lists the keys present on the input parent s3 key
    """
    fs = s3fs.S3FileSystem()
    return fs.ls(parent_key)

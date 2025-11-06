"""Tokenize the Paloma dataset with the GPT2 tokenizer.

Download the Paloma dataset:
```
cd data/
git lfs clone https://huggingface.co/datasets/allenai/paloma 
cd ..
```

Tokenize with this script:
```
uv run python tools/tokenize/paloma.py \
    data/paloma/c4_100_domains/val/ \
    data/paloma_c4_100_domains_val.bin
```

"""

import argparse
import functools
import gzip
import json
import multiprocessing
from pathlib import Path

import numpy as np
from tqdm import tqdm
from transformers import AutoTokenizer

_CHUNK_SIZE = 100
_GPT_VERSION = "gpt2"
_JSONL_GZ_GLOB = "*.jsonl.gz"
_TEXT_KEY = "text"


def tokenize_lines_and_add_eos(tokenizer: AutoTokenizer, path: Path) -> list[int]:
    """Opens the jsonl.gz file and tokenizes each line."""
    tokens = []
    with gzip.open(path, "rt") as f:
        for line in f:
            if not line.strip():
                continue  # skip empty lines
            contents = json.loads(line)
            tokens.extend(tokenizer.encode(contents[_TEXT_KEY]) + [tokenizer.eos_token_id])
    return tokens


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_dir",
        type=Path,
        help="The path to the paloma gzip files to tokenize, e.g. 'data/paloma/c4_100_domains/val'",
    )
    parser.add_argument(
        "output_file",
        type=Path,
        help="The path to the output binary file, e.g. 'data/paloma_c4_100_domains_val.bin'.",
    )
    parser.add_argument(
        "--num_processes",
        type=int,
        default=4,
        help="Number of processes to use for tokenization.",
    )
    args = parser.parse_args()

    # Following closely the script provided in assignment 4 pdf.
    tokenizer = AutoTokenizer.from_pretrained(_GPT_VERSION)
    tokenize_fn = functools.partial(tokenize_lines_and_add_eos, tokenizer)
    files = list(args.input_dir.glob(_JSONL_GZ_GLOB))
    pool = multiprocessing.Pool(args.num_processes)
    results = []
    for result in tqdm(
        pool.imap(tokenize_fn, files, chunksize=_CHUNK_SIZE),
        desc="Tokenizing lines",
    ):
        results.append(result)
    pool.close()
    pool.join()
    # Flatten the list of ids and convert to numpy array
    all_ids = [token_id for sublist in results for token_id in sublist]
    print(f"Tokenized and encoded {args.input_dir} into {len(all_ids)} tokens")
    ids_array = np.array(all_ids, dtype=np.uint16)
    ids_array.tofile(args.output_file)

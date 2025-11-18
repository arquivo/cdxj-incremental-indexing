#!/usr/bin/env python3
"""
cdxj_to_zipnum.py

Build local ZipNum-style index files and .loc entries from a single CDX/CDXJ
input (or stdin), without Hadoop/MRJob dependencies.

Usage examples (placed here for quick reference):

  # 1) Read from a single file with default 100MB shards (default chunk-size 3000):
  python cdxj_to_zipnum.py -o outdir -i input.cdxj

  # 2) Read from stdin (merged CDXJ piped in), write shards with custom chunk size:
  cat merged.cdxj | python cdxj_to_zipnum.py -o outdir -i -

  # 3) Read a gzipped CDXJ input file:
  python cdxj_to_zipnum.py -o outdir -i input.cdxj.gz

  # 4) Specify target shard size (200MB) and custom chunk size:
  python cdxj_to_zipnum.py -o outdir -i input.cdxj -s 200 -c 3000

  # 5) Custom base name and custom idx/loc filenames:
  python cdxj_to_zipnum.py -o outdir -i input.cdxj --base myindex --idx-file myindex.idx --loc-file myindex.loc

  # 6) Smaller shard size (50MB) and read from stdin:
  zcat many.cdxj.gz | python cdxj_to_zipnum.py -o outdir -i - -s 50

  # 7) Create a single shard file regardless of size:
  python cdxj_to_zipnum.py -o outdir -i input.cdxj --single-shard

Notes:
- Input may be '-' to read from stdin (uncompressed stream) or a path to a plain or .gz CDX/CDXJ file.
- Outputs are gzipped shard files (e.g. <base>-01.cdx.gz, <base>-02.cdx.gz, etc).
- If only one shard is created (either via --single-shard or input < shard-size), the file is named <base>.cdx.gz.
- Shards are created dynamically as the input is processed, each approximately the specified size (default 100MB).
- The produced .idx entries record compressed offsets and lengths (so they can be used with HTTP range requests
  against the compressed shard files).
- Default chunk size is 3000 lines and default shard size is 100MB (same as WARC files).

Pywb notes:
- By default pywb just reads a maximum of 10 chunks / blocks, if you generate more than that increase the `max_blocks`.

Description:
- Reads a single input CDX/CDXJ (plain or .gz, or stdin) and splits it into logical chunks
  (chunk-size lines each). Chunks are written sequentially to shard files to maintain sorted order
  for binary search. Each logical chunk is written into its shard gzip file, and the compressed 
  start offset and compressed length are recorded in an index (.idx). A .loc file maps shard names 
  to their file paths.

Author: Ivo Branco / Copilot
"""

from argparse import ArgumentParser
import os
import gzip
import sys
from typing import List, BinaryIO, Iterable, Tuple

def open_input_path(path: str) -> BinaryIO:
    """Open input for binary reading. Supports '-' (stdin) and .gz files."""
    if path == '-':
        return sys.stdin.buffer
    if path.endswith('.gz'):
        return gzip.open(path, 'rb')
    return open(path, 'rb')

def extract_prejson(line_bytes: bytes) -> str:
    """
    Return the CDXJ pre-JSON portion of a line.
    If a '{' exists in the line, returns everything before the first '{'.
    Otherwise returns the entire line (trimmed).
    """
    line = line_bytes.decode('utf-8', errors='replace').rstrip('\r\n')
    idx = line.find('{')
    if idx != -1:
        return line[:idx].strip()
    return line.strip()

def stream_chunks_from_input(input_path: str, chunk_size: int) -> Iterable[Tuple[int, List[bytes]]]:
    """
    Generator that yields (chunk_index, list_of_line_bytes) by reading a single input.
    Supports input_path == '-' for stdin, or a filename (plain or .gz).
    """
    chunk: List[bytes] = []
    chunk_idx = 0
    with open_input_path(input_path) as fh:
        for line in fh:
            chunk.append(line)
            if len(chunk) >= chunk_size:
                yield (chunk_idx, chunk)
                chunk_idx += 1
                chunk = []
    if chunk:
        yield (chunk_idx, chunk)

def ensure_dir(path: str):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

def cdxj_to_zipnum(output_dir: str, input_path: str, shard_size_mb: int = 100,
                       chunk_size: int = 3000, base: str = None,
                       idx_name: str = None, loc_name: str = None, compress_level: int = 6):
    """
    Main logic:
    - open shard gzip files for writing
    - stream input lines into logical chunks (chunk_size)
    - assign each chunk to a shard sequentially (fill one shard before moving to next)
    - write the chunk into the shard gzip file, recording compressed offsets and lengths
    - write index (.idx) and location (.loc) files at the end
    """
    ensure_dir(output_dir)
    if base is None:
        base = os.path.basename(os.path.abspath(output_dir)) or "zipnum-output"

    # Write idx file as we go (streaming) to avoid memory buildup
    if idx_name is None:
        idx_name = os.path.join(output_dir, f"{base}.idx")
    else:
        idx_name = os.path.join(output_dir, idx_name)
    
    chunk_generator = stream_chunks_from_input(input_path, chunk_size)
    
    # Target size per shard in bytes
    target_shard_size = shard_size_mb * 1024 * 1024
    
    # Track shard files created dynamically
    created_shards = []
    
    # Only keep current shard file handle open (memory efficient)
    current_shard = 0
    current_raw_fh = None
    
    # Helper function to generate shard path
    def get_shard_path(shard_num: int, is_single: bool = False) -> str:
        if is_single:
            # For single shard, use simple naming without numbering
            return os.path.join(output_dir, f"{base}.cdx.gz")
        else:
            return os.path.join(output_dir, f"{base}-{shard_num+1:02d}.cdx.gz")
    
    # Open first shard (we'll rename if it ends up being single shard)
    # Use larger buffer for better I/O performance
    shard_path = get_shard_path(current_shard)
    created_shards.append(shard_path)
    current_raw_fh = open(shard_path, 'wb', buffering=65536)
    
    # Iterate chunks in order and write sequentially to shards
    # This maintains the sorted order for binary search
    
    # Buffer for idx writes to reduce I/O calls
    idx_buffer = []
    idx_buffer_size = 100  # Write idx in batches of 100 entries
    
    with open(idx_name, 'w', encoding='utf-8', buffering=65536) as idxf:
        for _, chunk_lines in chunk_generator:
            # Compressed start offset (bytes) inside the gz file
            start_offset = current_raw_fh.tell()
            
            # Compress this chunk as an independent gzip member
            # This is critical for ZipNum: each chunk must be separately decompressible
            # Use configurable compression level (default 6 for better speed/compression balance)
            chunk_data = b''.join(chunk_lines)
            compressed_chunk = gzip.compress(chunk_data, compresslevel=compress_level)
            
            # Write compressed chunk to file
            current_raw_fh.write(compressed_chunk)
            end_offset = current_raw_fh.tell()
            comp_len = end_offset - start_offset

            # Extract pre-JSON from first line in chunk for index key
            pre = extract_prejson(chunk_lines[0])

            shard_basename = os.path.basename(created_shards[current_shard])
            # Remove .cdx.gz extension from shard name for idx file
            shard_name_no_ext = shard_basename.replace('.cdx.gz', '')
            
            # Buffer idx entries to reduce I/O calls
            idx_buffer.append(f"{pre}\t{shard_name_no_ext}\t{start_offset}\t{comp_len}\t{current_shard + 1}\n")
            
            if len(idx_buffer) >= idx_buffer_size:
                idxf.write(''.join(idx_buffer))
                idx_buffer.clear()
            
            # Check if current shard has reached target size
            # Move to next shard if current one is >= target size
            if end_offset >= target_shard_size:
                # Flush idx buffer before closing shard
                if idx_buffer:
                    idxf.write(''.join(idx_buffer))
                    idx_buffer.clear()
                
                # Close current shard file
                try:
                    current_raw_fh.close()
                except Exception:
                    pass
                
                # Move to next shard and open new file with larger buffer
                current_shard += 1
                shard_path = get_shard_path(current_shard)
                created_shards.append(shard_path)
                current_raw_fh = open(shard_path, 'wb', buffering=65536)
        
        # Flush any remaining idx entries
        if idx_buffer:
            idxf.write(''.join(idx_buffer))

    # Close final shard file
    try:
        current_raw_fh.close()
    except Exception:
        pass
    
    # If only one shard was created, rename it to use simple naming (no numbering)
    if len(created_shards) == 1 and not created_shards[0].endswith(f"{base}.cdx.gz"):
        simple_name = get_shard_path(0, is_single=True)
        os.rename(created_shards[0], simple_name)
        created_shards[0] = simple_name

    # Write loc file
    if loc_name is None:
        loc_name = os.path.join(output_dir, f"{base}.loc")
    else:
        loc_name = os.path.join(output_dir, loc_name)
    with open(loc_name, 'w', encoding='utf-8') as locf:
        for path in created_shards:
            basename = os.path.basename(path)
            # Remove .cdx.gz extension from first column
            shard_name = basename.replace('.cdx.gz', '')
            # Format: <shard_name>\t<relative_path>\n
            locf.write(f"{shard_name}\t{basename}\n")

    print(f"Finished. Wrote {len(created_shards)} shard file(s), index: {idx_name}, loc: {loc_name}")

def parse_args(argv=None):
    p = ArgumentParser(description="Build local ZipNum-style index files from a single CDX/CDXJ input (or stdin).")
    # Require explicit -i/--input and -o/--output flags (no positional args)
    p.add_argument('-i', '--input', required=True,
                   help="Input CDX/CDXJ file path, or '-' to read from stdin")
    p.add_argument('-o', '--output', required=True,
                   help='Output directory for shards, idx and loc')
    p.add_argument('-s', '--shard-size', type=int, default=100, 
                   help='Target size in MB for each shard file (default: 100MB, same as WARC files). Ignored if --single-shard is used.')
    p.add_argument('--single-shard', action='store_true',
                   help='Create a single shard file regardless of size (useful for small inputs or testing)')
    p.add_argument('-c', '--chunk-size', type=int, default=3000, help='Lines per chunk (default: 3000)')
    p.add_argument('--compress-level', type=int, default=6, choices=range(1, 10),
                   help='Gzip compression level 1-9 (default: 6). Lower=faster, higher=smaller. Level 6 offers best speed/size balance.')
    p.add_argument('--base', type=str, default=None, help='Base name for output files (default: basename of output dir)')
    p.add_argument('--idx-file', type=str, default=None, help='Custom index filename (written inside output dir)')
    p.add_argument('--loc-file', type=str, default=None, help='Custom loc filename (written inside output dir)')
    return p.parse_args(argv)

def main(argv=None):
    args = parse_args(argv)
    # If single-shard mode, use a very large shard size to ensure everything fits in one shard
    shard_size = float('inf') if args.single_shard else args.shard_size
    cdxj_to_zipnum(args.output, args.input, shard_size_mb=shard_size,
                       chunk_size=args.chunk_size, base=args.base,
                       idx_name=args.idx_file, loc_name=args.loc_file,
                       compress_level=args.compress_level)

if __name__ == "__main__":
    main()
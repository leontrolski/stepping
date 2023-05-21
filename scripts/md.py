from collections import defaultdict
import hashlib
from pathlib import Path
from textwrap import dedent
import time
import sys


def get_code(p: Path, reference: str) -> str:
    lines = list[str]()
    add_line = False
    for line in p.open():
        if f"# reference: {reference}" in line:
            add_line = True
            continue
        if f"# /reference: {reference}" in line:
            add_line = False
        if add_line:
            lines.append(line)
    if not lines:
        raise RuntimeError(f"Couldn't see any: '# reference: {reference}'")
    return dedent("".join(lines))


def line_to_reference(line: str) -> tuple[Path, str] | None:
    if line.startswith("```") and"[" in line and "]" in line:
        _, right = line.split("[")
        centre, _ = right.split("]")
        code_path_str, reference = centre.split("::")
        code_path_str = code_path_str.lstrip("/")
        return Path(code_path_str), reference
    return None


def rewrite(code_root: Path, md: Path) -> str:
    lines = list[str]()
    in_code_block = False
    for line in md.open():
        if not in_code_block:
            lines.append(line)

        if line.startswith("```") and in_code_block:
            lines.append(line)
            in_code_block = not in_code_block

        ref = line_to_reference(line)
        if ref is not None:
            in_code_block = True
            code_path, reference = ref
            code_path = (code_root / code_path).absolute()
            code = get_code(code_path, reference)
            lines.append(code)

    return "".join(lines)


def hash_(p: Path) -> str:
    return hashlib.md5(p.read_bytes()).hexdigest()

if __name__ == "__main__":
    _, code_root_str, *md_path_strs = sys.argv
    code_root = Path(code_root_str).absolute()
    if not code_root.exists():
        raise RuntimeError(f"No code root: {code_root}")

    code_md_map = dict[Path, Path]()
    md_codes_map: dict[Path, set[Path]] = defaultdict(set)
    for md_path_str in md_path_strs:
        md_path = Path(md_path_str).absolute()
        if not md_path.exists():
            raise RuntimeError(f"No markdown file: {md_path}")
        for line in md_path.open():
            ref = line_to_reference(line)
            if ref is None:
                continue
            code_path, _ = ref
            code_path = (code_root / code_path).absolute()
            if not code_path.exists():
                raise RuntimeError(f"No markdown file: {code_path}")
            code_md_map[code_path] = md_path
            md_codes_map[md_path].add(code_path)

    all_paths = set[Path]()
    for k, v in code_md_map.items():
        all_paths.add(k)
        all_paths.add(v)
    hashes = {p: "" for p in all_paths}

    while True:
        for p in all_paths:
            h = hash_(p)
            if h != hashes[p]:
                if p in code_md_map:
                    md_path = code_md_map[p]
                    code_paths = {p}
                else:
                    md_path = p
                    code_paths = md_codes_map[p]

                print(f"Rewriting {p}")
                new_md = rewrite(code_root, md_path)
                md_path.write_text(new_md)
                hashes[p] = hash_(p)
                break
        time.sleep(0.5)

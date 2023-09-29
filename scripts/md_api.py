from collections import defaultdict
import inspect

import stepping
from stepping import types
from stepping.operators.builder import traverse

by_module: dict[str, list[str]] = defaultdict(list)

for name in sorted(dir(stepping)):
    if name.startswith("_") or name.endswith("_"):
        continue
    if name.endswith("_lifted"):
        continue
    f = getattr(stepping, name)
    signature: types.Signature
    try:
        signature = traverse.get_signature(f)
    except (TypeError, ValueError, AttributeError):
        continue

    args = [f"{k}: {n}" for k, n in signature.args]

    if signature.kwargs:
        args += ["*"]
        args += [f"{k}: {n}" for k, n in signature.kwargs.items()]
    args_str = "".join(f"    {arg},\n" for arg in args)
    sig_str = f"st.{name}(\n{args_str}) -> {signature.ret}"

    for a, b in [
        ("stepping.types.", "st."),
        ("stepping.graph.", "st."),
        ("typing.", ""),
        ("~", ""),
        ("<class '", ""),
        ("'>", ""),
        (
            "pydantic.functional_serializers.PlainSerializer, pydantic.functional_validators.PlainValidator",
            "pydantic.Validator, ...",
        ),
        (
            "str | int | float | bool | None | datetime.date | datetime.datetime | uuid.UUID | tuple[str | int | float | bool | None | datetime.date | datetime.datetime | uuid.UUID, ...",
            "K",
        ),
    ]:
        sig_str = sig_str.replace(a, b)

    by_module[f.__module__].append(sig_str)

for module_name, sig_strs in sorted(by_module.items()):
    print("## " + module_name)
    print()
    for sig_str in sig_strs:
        print("```python")
        print(sig_str)
        # print("\n".join("    " + line for line in sig_str.splitlines()))
        print("```")
        print()

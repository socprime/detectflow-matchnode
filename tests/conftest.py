import sys
import types
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def _install_pyflink_stubs_if_needed() -> None:
    """Install lightweight PyFlink stubs when apache-flink extra is not installed.

    Operator unit tests only need OutputTag / Types / state descriptors / the
    KeyedBroadcastProcessFunction base class — not a real Flink runtime.
    """
    try:
        import pyflink.common.typeinfo  # noqa: F401

        return
    except ModuleNotFoundError:
        pass

    # site-packages may contain a leftover ``pyflink/log`` directory from local
    # Flink runs; treat that as "not installed" and overlay stubs.
    def _mod(name: str) -> types.ModuleType:
        module = types.ModuleType(name)
        sys.modules[name] = module
        return module

    pyflink = sys.modules.get("pyflink") or _mod("pyflink")
    common = _mod("pyflink.common")
    typeinfo = _mod("pyflink.common.typeinfo")
    datastream = _mod("pyflink.datastream")
    functions = _mod("pyflink.datastream.functions")
    state = _mod("pyflink.datastream.state")

    class _Types:
        @staticmethod
        def STRING():  # noqa: N802 — mirrors pyflink.common.typeinfo.Types API
            return "STRING"

        @staticmethod
        def LONG():  # noqa: N802
            return "LONG"

        @staticmethod
        def INT():  # noqa: N802
            return "INT"

        @staticmethod
        def ROW(field_types):  # noqa: N802
            return ("ROW", tuple(field_types))

    class Row:
        """Positional stand-in for pyflink.common.Row."""

        def __init__(self, *args):
            self._values = args

        def __getitem__(self, index: int):
            return self._values[index]

        def __len__(self) -> int:
            return len(self._values)

        def __iter__(self):
            return iter(self._values)

        def __eq__(self, other: object) -> bool:
            return isinstance(other, Row) and self._values == other._values

        def __repr__(self) -> str:
            return f"Row{self._values!r}"

    class OutputTag:
        def __init__(self, name: str, type_info=None):
            self.name = name
            self.type_info = type_info

        def __repr__(self) -> str:
            return f"OutputTag({self.name!r})"

        def __eq__(self, other: object) -> bool:
            return isinstance(other, OutputTag) and self.name == other.name

        def __hash__(self) -> int:
            return hash(self.name)

    class KeyedBroadcastProcessFunction:
        pass

    class _Descriptor:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    typeinfo.Types = _Types
    common.Row = Row
    datastream.OutputTag = OutputTag
    functions.KeyedBroadcastProcessFunction = KeyedBroadcastProcessFunction
    state.BroadcastState = object
    state.ListStateDescriptor = _Descriptor
    state.MapStateDescriptor = _Descriptor
    state.ValueStateDescriptor = _Descriptor

    pyflink.common = common
    pyflink.datastream = datastream
    common.typeinfo = typeinfo
    datastream.functions = functions
    datastream.state = state


_install_pyflink_stubs_if_needed()

import enum
from datetime import UTC, date, datetime
from uuid import UUID

import pytest

from stepping import steppingpack
from stepping.types import Pair
from stepping.zset.python import ZSetPython


class Foo(steppingpack.Data):
    bar: int


class EnumA(enum.Enum):
    one = 1
    two = 2


class DataA(steppingpack.Data):
    a: str


class DataB(steppingpack.Data):
    x: int
    many: tuple[DataA, ...]


class DataC(steppingpack.Data):
    st_discriminant: str = "DataC"
    c: str


class DataD(steppingpack.Data):
    st_discriminant: str = "DataD"
    d: str


class DataE(steppingpack.Data):
    a: str
    b: int
    c: float
    d: bool | None
    e: date
    f: datetime
    g: UUID
    h: EnumA
    i: DataC | DataD
    j: DataB
    k: tuple[DataC, ...]
    l: frozenset[str]
    m: frozenset[DataB]
    n: ZSetPython[float]
    o: Pair[str, date]


class DataF(steppingpack.Data):
    a: tuple[str, float]


class DataG(steppingpack.Data):
    a: tuple["DataG", ...]


def test_typing() -> None:
    Foo(bar=1)
    # Foo(1)
    # Foo(bar="1", baz=1)  # type: ignore[arg-type,call-arg]


def test_make_schema_atom() -> None:
    assert steppingpack.make_schema(int) == steppingpack.SAtom(type="int")
    assert steppingpack.make_schema(None) == steppingpack.SAtom(type="none")
    assert steppingpack.make_schema(date) == steppingpack.SAtom(type="date")
    assert steppingpack.make_schema(datetime) == steppingpack.SAtom(type="datetime")


def test_make_schema_enum() -> None:
    assert steppingpack.make_schema(EnumA) == steppingpack.SUnion(
        type="enum",
        options=(
            steppingpack.SLiteral(value=steppingpack.SAtom(type="int"), literal=1),
            steppingpack.SLiteral(value=steppingpack.SAtom(type="int"), literal=2),
        ),
    )


def test_make_schema_composite() -> None:
    assert steppingpack.make_schema(tuple[int, ...]) == steppingpack.STuple(
        values=(steppingpack.SAtom(type="int"),), many=True
    )
    assert steppingpack.make_schema(tuple[EnumA, ...]) == steppingpack.STuple(
        values=(
            steppingpack.SUnion(
                type="enum",
                options=(
                    steppingpack.SLiteral(
                        value=steppingpack.SAtom(type="int"), literal=1
                    ),
                    steppingpack.SLiteral(
                        value=steppingpack.SAtom(type="int"), literal=2
                    ),
                ),
            ),
        ),
        many=True,
    )
    assert steppingpack.make_schema(frozenset[str]) == steppingpack.SFrozenset(
        value=steppingpack.SAtom(type="str")
    )
    assert steppingpack.make_schema(ZSetPython[str]) == steppingpack.SVariadic(
        value=steppingpack.STuple(
            values=(
                steppingpack.SAtom(type="str"),
                steppingpack.SAtom(type="int"),
            ),
            many=False,
        )
    )


def test_make_schema_union() -> None:
    assert steppingpack.make_schema(int | float) == steppingpack.SUnion(
        type="union",
        options=(steppingpack.SAtom(type="int"), steppingpack.SAtom(type="float")),
    )


def test_make_schema_data() -> None:
    assert steppingpack.make_schema(DataB) == steppingpack.SData(
        pairs=(
            steppingpack.SDataPair(
                key="x",
                value=steppingpack.SAtom(type="int"),
            ),
            steppingpack.SDataPair(
                key="many",
                value=steppingpack.STuple(
                    values=(
                        steppingpack.SData(
                            pairs=(
                                steppingpack.SDataPair(
                                    key="a",
                                    value=steppingpack.SAtom(type="str"),
                                    default=steppingpack.SNoValue(),
                                ),
                            ),
                        ),
                    ),
                    many=True,
                ),
            ),
        ),
    )


def test_make_schema_union_of_data() -> None:
    assert steppingpack.make_schema(DataC | DataD) == steppingpack.SUnion(
        type="union",
        options=(
            steppingpack.SData(
                type="data",
                pairs=(
                    steppingpack.SDataPair(
                        key="st_discriminant",
                        value=steppingpack.SAtom(type="str"),
                        default="DataC",
                    ),
                    steppingpack.SDataPair(
                        key="c",
                        value=steppingpack.SAtom(type="str"),
                        default=steppingpack.SNoValue(type="novalue"),
                    ),
                ),
                discriminant="DataC",
                discriminant_index=0,
            ),
            steppingpack.SData(
                type="data",
                pairs=(
                    steppingpack.SDataPair(
                        key="st_discriminant",
                        value=steppingpack.SAtom(type="str"),
                        default="DataD",
                    ),
                    steppingpack.SDataPair(
                        key="d",
                        value=steppingpack.SAtom(type="str"),
                        default=steppingpack.SNoValue(type="novalue"),
                    ),
                ),
                discriminant="DataD",
                discriminant_index=0,
            ),
        ),
    )


def test_make_schema_union_error() -> None:
    with pytest.raises(RuntimeError):
        assert steppingpack.make_schema(DataA | DataC)


def test_hash() -> None:
    d = DataB(x=42, many=(DataA(a="3"), DataA(a="4")))
    hash(d)


def thereandback(t: type[steppingpack.TValue], o: steppingpack.TValue) -> None:
    dumped = steppingpack.dump(o)
    loaded = steppingpack.load(t, dumped)
    assert o == loaded


def test_dump_basic() -> None:
    thereandback(DataA, DataA(a="3"))
    thereandback(DataB, DataB(x=42, many=(DataA(a="3"), DataA(a="4"))))
    thereandback(frozenset[str], frozenset(("sdf", "fdgdg")))
    thereandback(date, date(2022, 1, 3))
    thereandback(datetime, datetime(2022, 1, 3, tzinfo=UTC))
    thereandback(UUID, UUID("4c6c2692-6731-426d-b2c0-d08e672c8678"))
    thereandback(EnumA, EnumA.one)
    thereandback(DataF, DataF(a=("3", 3.14)))
    thereandback(DataF, DataF(a=("3", 3.0)))


def test_serialize_schema() -> None:
    schema = steppingpack.make_schema(DataE)
    serialized = steppingpack.serialize_schema(schema)
    back_out = steppingpack.deserialize_schema(serialized)
    assert schema == back_out


def test_all() -> None:
    d = DataE(
        a="a",
        b=42,
        c=54.0,
        d=None,
        e=date(2021, 5, 6),
        f=datetime(2021, 5, 6),
        g=UUID("4c6c2692-6731-426d-b2c0-d08e672c8678"),
        h=EnumA.one,
        i=DataC(c="c"),
        j=DataB(x=42, many=(DataA(a="3"), DataA(a="4"))),
        k=(DataC(c="ddd"),),
        l=frozenset(("str-4", "str-5", "str-6")),
        m=frozenset(
            (
                DataB(x=42, many=(DataA(a="3"), DataA(a="4"))),
                DataB(x=42, many=(DataA(a="3"), DataA(a="5"))),
            )
        ),
        n=ZSetPython({3.14: 4, 2.0: -1}),
        o=Pair("ssss", date(2012, 2, 3)),
    )
    assert (
        d.st_bytes
        == b"\x9f\xa1a*\xcb@K\x00\x00\x00\x00\x00\x00\xc0\xaa2021-05-06\xb32021-05-06T00:00:00\xd9$4c6c2692-6731-426d-b2c0-d08e672c8678\x01\x92\xa5DataC\xa1c\x92*\x92\x91\xa13\x91\xa14\x91\x92\xa5DataC\xa3ddd\x93\xa5str-4\xa5str-5\xa5str-6\x92\x92*\x92\x91\xa13\x91\xa14\x92*\x92\x91\xa13\x91\xa15\x92\x92\xcb@\x00\x00\x00\x00\x00\x00\x00\xff\x92\xcb@\t\x1e\xb8Q\xeb\x85\x1f\x04\x92\xa4ssss\xaa2012-02-03"
    )
    assert d.st_identifier == b"\xcb\nr\x8f\x0b\x14\xe7\x19\xb9`\xa0\xec>\"~'"
    hash(d)

    import cProfile

    with cProfile.Profile() as pr:
        for _ in range(1000):
            thereandback(DataE, d)
    pr.dump_stats("steppingpack.prof")


def test_recursive() -> None:
    schema = steppingpack.make_schema(DataG)
    assert schema == steppingpack.SData(
        pairs=(
            steppingpack.SDataPair(
                key="a",
                value=steppingpack.STuple(
                    values=(
                        steppingpack.SReference(
                            reference="tests.test_steppingpack.DataG"
                        ),
                    ),
                    many=True,
                ),
            ),
        ),
    )
    d = DataG(a=(DataG(a=()), DataG(a=(DataG(a=()),))))
    dumped = steppingpack.dump(d)
    assert steppingpack.load(DataG, dumped)

    serialized = steppingpack.serialize_schema(schema)
    back_out = steppingpack.deserialize_schema(serialized)
    assert schema == back_out

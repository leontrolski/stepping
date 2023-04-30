import cProfile
import random
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from uuid import UUID

import stepping.store
from stepping import graph, operators, run, types
from stepping.types import RuntimeComposite as R
from stepping.zset import postgres
from stepping.zset.python import ZSetPython


@dataclass(frozen=True)
class User(types.Data):
    user_id: int
    name: str


@dataclass(frozen=True)
class Meter(types.Data):
    meter_id: UUID
    user_id: int


@dataclass(frozen=True)
class HalfHourlyMeterRead(types.Data):
    meter_id: UUID
    timestamp: datetime
    value: float


user_1 = User(1, "oli")
meter_id_1 = UUID("00000001-b3fb-47ef-b22f-a45ba5b7b645")
meter_id_2 = UUID("00000002-b3fb-47ef-b22f-a45ba5b7b645")
meter_1 = Meter(meter_id_1, 1)
half_hourly_reads_1 = [
    HalfHourlyMeterRead(meter_id_1, datetime(2023, 1, 1, 2), 24.0),
    HalfHourlyMeterRead(meter_id_1, datetime(2023, 1, 1, 3), 0.0),
    HalfHourlyMeterRead(meter_id_1, datetime(2023, 1, 2, 4), 12.5),
    HalfHourlyMeterRead(meter_id_1, datetime(2023, 1, 3, 5), 8.0),
    HalfHourlyMeterRead(meter_id_1, datetime(2023, 1, 2, 6), 6.0),
    HalfHourlyMeterRead(meter_id_2, datetime(2023, 1, 2, 6), 100.0),
]


@dataclass(frozen=True)
class Flat(types.Data):
    user_id: int
    meter_id: UUID
    timestamp: datetime
    value: float
    date: date


def flatten(p: types.Pair[types.Pair[User, Meter], HalfHourlyMeterRead]) -> Flat:
    return Flat(
        user_id=p.left.left.user_id,
        meter_id=p.left.right.meter_id,
        timestamp=p.right.timestamp,
        value=p.right.value,
        date=p.right.timestamp.date(),
    )


@dataclass(frozen=True)
class DailyUsage(types.Data):
    user_id: int
    meter_id: UUID
    date: date
    value: float


@dataclass(frozen=True)
class CompositeKey(types.Data):
    user_id: int
    meter_id: UUID
    date: date


def to_daily(p: types.Pair[tuple[int, UUID, date], float]) -> DailyUsage:
    user_id, meter_id, date = p.left
    return DailyUsage(
        user_id=user_id,
        meter_id=meter_id,
        date=date,
        value=p.right,
    )


def test_profile(conn: postgres.Conn, request: Any) -> None:
    join1 = operators.join(
        User,
        Meter,
        types.pick_index(User, lambda u: u.user_id),
        types.pick_index(Meter, lambda m: m.user_id),
    )
    id1 = operators.identity_zset(
        HalfHourlyMeterRead,
    )
    join2 = operators.join(
        types.Pair[User, Meter],
        HalfHourlyMeterRead,
        types.pick_index(types.Pair[User, Meter], lambda p: p.right.meter_id),
        types.pick_index(HalfHourlyMeterRead, lambda m: m.meter_id),
    )
    map1 = operators.map(
        types.Pair[types.Pair[User, Meter], HalfHourlyMeterRead], Flat, flatten
    )

    index = types.pick_index(Flat, lambda f: (f.user_id, f.meter_id, f.date))
    group1 = operators.group(Flat, index)
    reduce1 = operators.reduce(Flat, float, pick_reducable=lambda f: f.value)
    reduce1_lifted = operators.lift_grouped(index.k, reduce1)
    flatten1 = operators.flatten(float, index.k)
    group_and_reduce = group1.connect(reduce1_lifted).connect(flatten1)

    map2 = operators.map(
        types.Pair[tuple[int, UUID, date], float], DailyUsage, to_daily
    )
    query = operators.finalize(
        graph.stack(join1, id1)
        .connect(join2)
        .connect(map1)
        .connect(group_and_reduce)
        .connect(map2)
    )
    if request.config.getoption("--write-graphs"):
        graph.write_png(query, "graphs/profile/test_profile.png", simplify_labels=False)

    store = stepping.store.StorePython.from_graph(query)
    # store = run.StoreSQL.from_graph(conn, query, "test_profile")

    so_far = set[tuple[int, DailyUsage]]()

    def insert(zs: tuple[types.ZSet[DailyUsage]]) -> None:
        (z,) = zs
        for v, count in z.iter():
            so_far.add((count, v))

    no_user = ZSetPython[User]()
    no_meter = ZSetPython[Meter]()
    no_read = ZSetPython[HalfHourlyMeterRead]()

    def insert_users(users: list[User]) -> None:
        insert(
            run.iteration(
                store, query, (ZSetPython({n: 1 for n in users}), no_meter, no_read)
            )
        )

    def insert_meter(n: Meter) -> None:
        insert(run.iteration(store, query, (no_user, ZSetPython({n: 1}), no_read)))

    def insert_reads(reads: list[HalfHourlyMeterRead]) -> None:
        insert(
            run.iteration(
                store, query, (no_user, no_meter, ZSetPython({n: 1 for n in reads}))
            )
        )

    insert_users([user_1])
    insert_meter(meter_1)
    for read in half_hourly_reads_1:
        insert_reads([read])

    actual = [(count, value) for count, value in so_far if value.value != 0.0]
    actual = sorted(actual, key=lambda t: (t[1].date, t[1].value, -t[0]))
    assert actual == [
        (1, DailyUsage(1, meter_id_1, date(2023, 1, 1), 24.0)),
        (1, DailyUsage(1, meter_id_1, date(2023, 1, 2), 12.5)),
        (
            -1,
            DailyUsage(1, meter_id_1, date(2023, 1, 2), 12.5),
        ),
        (1, DailyUsage(1, meter_id_1, date(2023, 1, 2), 18.5)),
        (1, DailyUsage(1, meter_id_1, date(2023, 1, 3), 8.0)),
    ]

    def make_random_read() -> HalfHourlyMeterRead:
        return HalfHourlyMeterRead(
            meter_id_1,
            datetime(2023, 1, random.randint(1, 31), random.randint(0, 23)),
            round(random.random() * 100, 2),
        )

    n = int(request.config.getoption("--n-profile"))
    insert_users([User(i + 10, f"user-{i + 10}") for i in range(n)])
    insert_reads([make_random_read() for _ in range(n)])

    with cProfile.Profile() as pr:
        insert_reads([make_random_read()])

    pr.dump_stats("insert_with_join.prof")

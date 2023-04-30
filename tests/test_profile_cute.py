from __future__ import annotations

import cProfile
import random
from dataclasses import dataclass
from datetime import date, datetime
from pprint import pprint
from typing import Any
from uuid import UUID

import stepping as st


@dataclass(frozen=True)
class User(st.Data):
    def identity(self) -> str:
        return str(self.user_id)

    user_id: int
    name: str


@dataclass(frozen=True)
class Meter(st.Data):
    def identity(self) -> str:
        return str(self.meter_id)

    meter_id: UUID
    user_id: int


@dataclass(frozen=True)
class HalfHourlyMeterRead(st.Data):
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
class UserMeter(User, Meter):
    ...


@dataclass(frozen=True)
class UserMeterRead(User, Meter, HalfHourlyMeterRead):
    date: date


def with_date(p: st.Pair[UserMeter, HalfHourlyMeterRead]) -> UserMeterRead:
    return UserMeterRead(
        user_id=p.left.user_id,
        name=p.left.name,
        meter_id=p.left.meter_id,
        timestamp=p.right.timestamp,
        value=p.right.value,
        date=p.right.timestamp.date(),
    )


@dataclass(frozen=True)
class DailyUsage(st.Data):
    user_id: int
    meter_id: UUID
    date: date
    value: float


def to_daily(p: st.Pair[tuple[int, UUID, date], float]) -> DailyUsage:
    user_id, meter_id, date = p.left
    return DailyUsage(
        user_id=user_id,
        meter_id=meter_id,
        date=date,
        value=p.right,
    )


def make_random_read() -> HalfHourlyMeterRead:
    return HalfHourlyMeterRead(
        meter_id_1,
        datetime(2023, 1, random.randint(1, 31), random.randint(0, 23)),
        round(random.random() * 100, 2),
    )


def test_profile_1(conn: st.Conn, request: Any) -> None:
    join_meters = st.stack(
        st.join_flat(
            User,
            Meter,
            st.pick_index(User, lambda u: u.user_id),
            st.pick_index(Meter, lambda m: m.user_id),
            UserMeter,
        ),
        st.identity_zset(HalfHourlyMeterRead),
    )
    join_reads = st.join(
        UserMeter,
        HalfHourlyMeterRead,
        st.pick_index(UserMeter, lambda p: p.meter_id),
        st.pick_index(HalfHourlyMeterRead, lambda m: m.meter_id),
    )
    merge = st.map(
        st.Pair[UserMeter, HalfHourlyMeterRead],
        UserMeterRead,
        with_date,
    )
    group = st.group_reduce_flatten(
        UserMeterRead,
        group_by=st.pick_index(
            UserMeterRead, lambda f: (f.user_id, f.meter_id, f.date)
        ),
        reduce_on=st.pick_field(UserMeterRead, lambda f: f.value),
    )
    as_daily = st.map(
        st.Pair[tuple[int, UUID, date], float],
        DailyUsage,
        to_daily,
    )
    query = st.finalize(
        join_meters.connect(join_reads).connect(merge).connect(group).connect(as_daily)
    )
    if request.config.getoption("--write-graphs"):
        st.write_png(query, "graphs/profile/test_profile_cute.png")

    # store = st.StorePython.from_graph(query)
    store = st.StorePostgres.from_graph(conn, query, "test_profile")
    i_users, i_meters, i_reads = st.actions(store, query)

    so_far = set[tuple[int, DailyUsage]]()

    def insert(zs: tuple[st.ZSet[DailyUsage]]) -> None:
        (z,) = zs
        for v, count in z.iter():
            so_far.add((count, v))

    def insert_users(users: list[User]) -> None:
        insert(i_users.insert(users))

    def insert_meter(n: Meter) -> None:
        insert(i_meters.insert([n]))

    def insert_reads(reads: list[HalfHourlyMeterRead]) -> tuple[Any, ...]:
        out = i_reads.insert(reads)
        insert(out)
        return out

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

    n = int(request.config.getoption("--n-profile"))
    insert_users([User(i + 10, f"user-{i + 10}") for i in range(n)])
    insert_reads([make_random_read() for _ in range(n)])

    random_reads = [make_random_read(), make_random_read()]
    with cProfile.Profile() as pr:
        check = insert_reads(random_reads)

    # print(random_reads)
    # pprint(list(check[0]._data.keys()))

    pr.dump_stats("insert_with_join.prof")


def print_time(pr: Any) -> None:
    print(sorted(pr.getstats(), key=lambda n: n.totaltime)[-1].totaltime)  # type: ignore


def test_integrate(conn: st.Conn, request: Any) -> None:
    join_meters = st.join_flat(
        User,
        Meter,
        st.pick_index(User, lambda u: u.user_id),
        st.pick_index(Meter, lambda m: m.user_id),
        UserMeter,
    )
    join_reads = st.join(
        UserMeter,
        HalfHourlyMeterRead,
        st.pick_index(UserMeter, lambda p: p.meter_id),
        st.pick_index(HalfHourlyMeterRead, lambda m: m.meter_id),
    )
    merge = st.map(
        st.Pair[UserMeter, HalfHourlyMeterRead],
        UserMeterRead,
        with_date,
    )
    group = st.group_reduce_flatten(
        UserMeterRead,
        group_by=st.pick_index(
            UserMeterRead, lambda f: (f.user_id, f.meter_id, f.date)
        ),
        reduce_on=st.pick_field(UserMeterRead, lambda f: f.value),
    )
    as_daily = st.map(
        st.Pair[tuple[int, UUID, date], float],
        DailyUsage,
        to_daily,
    )
    integrate = st.integrate_zset(DailyUsage)
    query = st.finalize(
        st.stack(
            join_meters,
            st.identity_zset(HalfHourlyMeterRead),
        )
        .connect(join_reads)
        .connect(merge)
        .connect(group)
        .connect(as_daily)
        .connect(integrate)
    )

    store = st.StorePostgres.from_graph(conn, query, "test_profile_integrate")

    no_user = st.ZSetPython[User]()
    no_meter = st.ZSetPython[Meter]()
    no_read = st.ZSetPython[HalfHourlyMeterRead]()

    def insert_users(users: list[User]) -> None:
        st.iteration(
            store, query, (st.ZSetPython({n: 1 for n in users}), no_meter, no_read)
        )

    def insert_meter(n: Meter) -> None:
        st.iteration(store, query, (no_user, st.ZSetPython({n: 1}), no_read))

    def insert_reads(reads: list[HalfHourlyMeterRead]) -> None:
        st.iteration(
            store, query, (no_user, no_meter, st.ZSetPython({n: 1 for n in reads}))
        )

    insert_users([user_1])
    insert_meter(meter_1)
    insert_reads(half_hourly_reads_1)

    n = int(request.config.getoption("--n-profile"))

    # with cProfile.Profile() as pr:
    insert_users([User(i + 10, f"user-{i + 10}") for i in range(n)])
    insert_reads([make_random_read() for _ in range(n)])
    # pr.dump_stats("many.prof")

    random_reads = [make_random_read(), make_random_read()]
    with cProfile.Profile() as pr:
        insert_reads(random_reads)

    pr.dump_stats("insert_with_join.prof")

    qry = """
        SELECT data, c 
        FROM test_profile_integrate__delay__integrate_n9 
        WHERE (data #>> '{user_id}')::int = 1
    """

    with cProfile.Profile() as pr:
        rows = list(conn.execute(qry))

    pr.dump_stats("read.prof")
    print("\ntest_profile_integrate took:")
    print_time(pr)
    pprint(rows[:3])


def test_classic(conn: st.Conn, request: Any) -> None:
    qry = """
    CREATE TABLE user_ (
        user_id INT PRIMARY KEY,
        name TEXT
    );
    CREATE TABLE meter (
        meter_id UUID PRIMARY KEY,
        user_id INT
    );
    CREATE INDEX ix_meter_user_id ON meter (user_id);
    CREATE TABLE read (
        meter_id UUID,
        timestamp TIMESTAMP,
        value FLOAT
    );
    CREATE INDEX ix_read_meter_id ON read (meter_id);
    """
    conn.execute(qry)

    def insert_user(user: User) -> None:
        conn.execute("INSERT INTO user_ VALUES (%s, %s)", [user.user_id, user.name])

    def insert_meter(meter: Meter) -> None:
        conn.execute(
            "INSERT INTO meter VALUES (%s, %s)", [meter.meter_id, meter.user_id]
        )

    def insert_read(read: HalfHourlyMeterRead) -> None:
        conn.execute(
            "INSERT INTO read VALUES (%s, %s, %s)",
            [read.meter_id, read.timestamp, read.value],
        )

    insert_user(user_1)
    insert_meter(meter_1)
    for read in half_hourly_reads_1:
        insert_read(read)

    n = int(request.config.getoption("--n-profile"))

    for i in range(n):
        insert_user(User(i + 10, f"user-{i + 10}"))
    for _ in range(n):
        insert_read(make_random_read())

    qry = """
        SELECT 
            user_.user_id, 
            meter.meter_id,
            read.timestamp::date AS date_,
            sum(read.value) AS daily
        FROM user_
        JOIN meter USING (user_id)
        JOIN read USING (meter_id)
        GROUP BY user_id, meter_id, read.timestamp::date
    """

    conn.commit()

    with cProfile.Profile() as pr:
        rows = list(conn.execute(qry))

    pr.dump_stats("classic.prof")

    print("\ntest_classic took:")
    print_time(pr)
    pprint(rows[:3])

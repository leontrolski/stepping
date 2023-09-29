import concurrent.futures
import cProfile
import random
import time
from datetime import date, datetime
from pprint import pprint
from typing import Any, Callable
from unittest.mock import ANY
from uuid import UUID

import pytest

import stepping as st
from tests.conftest import DB_URL, Conns
from tests.helpers import StoreMaker, store_ids, store_makers


class User(st.Data):
    user_id: int
    name: str


class Meter(st.Data):
    meter_id: UUID
    user_id: int


class HalfHourlyMeterRead(st.Data):
    meter_id: UUID
    timestamp: datetime
    value: float


class UserMeter(User, Meter):
    ...


def make_user_meter(p: st.Pair[User, Meter]) -> UserMeter:
    return UserMeter(
        user_id=p.left.user_id,
        name=p.left.name,
        meter_id=p.right.meter_id,
    )


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


class DailyUsage(st.Data):
    user_id: int
    meter_id: UUID
    date: date
    value: float


def to_daily(p: st.Pair[float, tuple[int, UUID, date]]) -> DailyUsage:
    user_id, meter_id, date = p.right
    return DailyUsage(
        user_id=user_id,
        meter_id=meter_id,
        date=date,
        value=p.left,
    )


def pick_value(u: UserMeterRead) -> float:
    return u.value


def _f_test_profile_1(
    users: st.ZSet[User],
    meters: st.ZSet[Meter],
    reads: st.ZSet[HalfHourlyMeterRead],
) -> st.ZSet[DailyUsage]:
    join_meters = st.join(
        users,
        meters,
        on_left=st.pick_index(User, lambda u: u.user_id),
        on_right=st.pick_index(Meter, lambda m: m.user_id),
    )
    join_meters_flat = st.map(join_meters, f=make_user_meter)
    join_reads = st.join(
        join_meters_flat,
        reads,
        on_left=st.pick_index(UserMeter, lambda p: p.meter_id),
        on_right=st.pick_index(HalfHourlyMeterRead, lambda m: m.meter_id),
    )
    merged = st.map(join_reads, f=with_date)
    grouped = st.group(
        merged,
        by=st.pick_index(UserMeterRead, lambda f: (f.user_id, f.meter_id, f.date)),
    )
    reduced = st.per_group[grouped](
        lambda g: st.reduce(g, zero=float, pick_value=pick_value)
    )
    flattened = st.flatten(reduced)
    as_daily = st.map(flattened, f=to_daily)
    return as_daily


index_daily = st.pick_index(DailyUsage, lambda d: d.date)
daily_cache = st.Cache[DailyUsage]()


def _f_test_integrate(
    users: st.ZSet[User],
    meters: st.ZSet[Meter],
    reads: st.ZSet[HalfHourlyMeterRead],
) -> st.ZSet[DailyUsage]:
    as_daily = _f_test_profile_1(users, meters, reads)
    _ = daily_cache[as_daily](lambda a: st.integrate_indexed(a, indexes=(index_daily,)))
    return as_daily


user_1 = User(user_id=1, name="oli")
meter_id_1 = UUID("00000001-b3fb-47ef-b22f-a45ba5b7b645")
meter_id_2 = UUID("00000002-b3fb-47ef-b22f-a45ba5b7b645")
meter_1 = Meter(meter_id=meter_id_1, user_id=1)
half_hourly_reads_1 = [
    HalfHourlyMeterRead(
        meter_id=meter_id_1, timestamp=datetime(2023, 1, 1, 2), value=24.0
    ),
    HalfHourlyMeterRead(
        meter_id=meter_id_1, timestamp=datetime(2023, 1, 1, 3), value=0.0
    ),
    HalfHourlyMeterRead(
        meter_id=meter_id_1, timestamp=datetime(2023, 1, 2, 4), value=12.5
    ),
    HalfHourlyMeterRead(
        meter_id=meter_id_1, timestamp=datetime(2023, 1, 3, 5), value=8.0
    ),
    HalfHourlyMeterRead(
        meter_id=meter_id_1, timestamp=datetime(2023, 1, 2, 6), value=6.0
    ),
    HalfHourlyMeterRead(
        meter_id=meter_id_2, timestamp=datetime(2023, 1, 2, 6), value=100.0
    ),
]


def make_random_read(r: random.Random = random) -> HalfHourlyMeterRead:  # type: ignore[assignment]
    return HalfHourlyMeterRead(
        meter_id=meter_id_1,
        timestamp=datetime(2023, 1, r.randint(1, 31), r.randint(0, 23)),
        value=round(r.random() * 100, 2),
    )


def test_profile_1(postgres_conn: st.ConnPostgres, request: Any) -> None:
    query = st.compile(_f_test_profile_1)
    if request.config.getoption("--write-graphs"):
        st.write_png(query, "graphs/test_profile_1.png")

    # store = st.StorePython.from_graph(query)
    store = st.StorePostgres.from_graph(postgres_conn, query, create_tables=True)
    i_users, i_meters, i_reads = st.actions(store, query)

    so_far = set[tuple[int, DailyUsage]]()

    def insert(zs: tuple[st.ZSet[DailyUsage]]) -> None:
        (z,) = zs
        for v, count in z.iter():
            so_far.add((count, v))

    def insert_users(users: list[User]) -> None:
        insert(i_users.insert(*users))

    def insert_meter(n: Meter) -> None:
        insert(i_meters.insert(*[n]))

    def insert_reads(reads: list[HalfHourlyMeterRead]) -> tuple[Any, ...]:
        out = i_reads.insert(*reads)
        insert(out)
        return out

    insert_users([user_1])
    insert_meter(meter_1)
    for read in half_hourly_reads_1:
        insert_reads([read])

    actual = [(count, value) for count, value in so_far if value.value != 0.0]
    actual = sorted(actual, key=lambda t: (t[1].date, t[1].value, -t[0]))
    assert actual == [
        (
            1,
            DailyUsage(
                user_id=1, meter_id=meter_id_1, date=date(2023, 1, 1), value=24.0
            ),
        ),
        (
            1,
            DailyUsage(
                user_id=1, meter_id=meter_id_1, date=date(2023, 1, 2), value=12.5
            ),
        ),
        (
            -1,
            DailyUsage(
                user_id=1, meter_id=meter_id_1, date=date(2023, 1, 2), value=12.5
            ),
        ),
        (
            1,
            DailyUsage(
                user_id=1, meter_id=meter_id_1, date=date(2023, 1, 2), value=18.5
            ),
        ),
        (
            1,
            DailyUsage(
                user_id=1, meter_id=meter_id_1, date=date(2023, 1, 3), value=8.0
            ),
        ),
    ]

    ns = list(range(int(request.config.getoption("--n-profile"))))
    insert_users([User(user_id=i + 10, name=f"user-{i + 10}") for i in ns])
    insert_reads([make_random_read() for _ in ns])

    random_reads = [make_random_read(), make_random_read()]
    with cProfile.Profile() as pr:
        check = insert_reads(random_reads)

    # print(random_reads)
    # pprint(list(check[0]._data.keys()))

    pr.dump_stats("test_profile_1.prof")


def print_time(pr: Any) -> None:
    print(sorted(pr.getstats(), key=lambda n: n.totaltime)[-1].totaltime)


@pytest.mark.parametrize("store_maker", store_makers, ids=store_ids)
def test_integrate(request: Any, conns: Conns, store_maker: StoreMaker) -> None:
    graph, store = store_maker(conns, _f_test_integrate)

    assert daily_cache.vertex_delay is not None
    i_users, i_meters, i_reads = st.actions(store, graph)
    i_users.insert(user_1)
    i_meters.insert(meter_1)
    i_reads.insert(*half_hourly_reads_1)

    ns = list(range(int(request.config.getoption("--n-profile"))))
    i_users.insert(*[User(user_id=i + 10, name=f"user-{i + 10}") for i in ns])
    i_reads.insert(*[make_random_read() for _ in ns])

    random_reads = [make_random_read(), make_random_read()]

    with cProfile.Profile() as pr:
        i_reads.insert(*random_reads)

    pr.dump_stats("test_integrate.prof")

    if isinstance(store, st.StoreSQL) and store._zset_cls is st.ZSetPostgres:
        qry = """
            SELECT data, c
            FROM t__sd_795b2c
            WHERE (data #>> '{user_id}')::int = 1
        """

        with cProfile.Profile() as pr:
            rows = list(conns.postgres.execute(qry))

        pr.dump_stats("test_integrate__read.prof")
        print("\ntest_profile_integrate took:")
        print_time(pr)
        pprint(rows[:3])

    actual = list(
        daily_cache.zset(store).iter_by_index(
            index_daily, frozenset((date(2023, 1, 2),))
        )
    )

    # This was tested like this pre-pydantic
    # usage = DailyUsage(
    #     user_id=1,
    #     meter_id=UUID("00000001-b3fb-47ef-b22f-a45ba5b7b645"),
    #     date=date(2023, 1, 2),
    #     value=ANY,
    # )
    assert actual == [(date(2023, 1, 2), ANY, 1)]


daily_cache_parallel = st.Cache[DailyUsage]()


def _f_test_parallel(
    users: st.ZSet[User],
    meters: st.ZSet[Meter],
    reads: st.ZSet[HalfHourlyMeterRead],
) -> st.ZSet[DailyUsage]:
    as_daily = _f_test_profile_1(users, meters, reads)
    _ = daily_cache_parallel[as_daily](
        lambda a: st.integrate_indexed(a, indexes=(index_daily,))
    )
    return as_daily


def worker(reads: list[HalfHourlyMeterRead]) -> None:
    with st.connection_postgres(DB_URL) as conn:
        graph = st.compile(_f_test_parallel)
        store = st.StorePostgres.from_graph(conn, graph, create_tables=False)
        _, __, i_reads = st.actions(store, graph)
        i_reads.insert(*reads)


@pytest.mark.skip
def test_parallel(request: Any, postgres_conn: st.ConnPostgres) -> None:
    graph = st.compile(_f_test_parallel)
    store = st.StorePostgres.from_graph(postgres_conn, graph, create_tables=True)
    i_users, i_meters, i_reads = st.actions(store, graph)
    i_users.insert(user_1)
    i_meters.insert(meter_1)
    i_reads.insert(*half_hourly_reads_1)

    n = int(request.config.getoption("--n-profile"))
    ns = list(range(n))
    r = random.Random(42)
    reads = [make_random_read(r) for _ in ns]
    batched = list(st.batched(reads, n=100))

    # i_reads.insert(*reads)

    before = time.time()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for _ in executor.map(worker, batched):
            pass
    print(f"Took {time.time() - before}s")

    if n == 1000:
        actual = list(
            daily_cache_parallel.zset(store).iter_by_index(
                index_daily, frozenset((date(2023, 1, 2),))
            )
        )
        usage = DailyUsage(
            user_id=1,
            meter_id=UUID("00000001-b3fb-47ef-b22f-a45ba5b7b645"),
            date=date(2023, 1, 2),
            value=1202.13,
        )
        assert actual == [(date(2023, 1, 2), usage, 1)]


def test_classic(postgres_conn: st.ConnPostgres | st.ConnSQLite, request: Any) -> None:
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
    postgres_conn.execute(qry)

    def insert_user(user: User) -> None:
        postgres_conn.execute(
            "INSERT INTO user_ VALUES (%s, %s)", [user.user_id, user.name]
        )

    def insert_meter(meter: Meter) -> None:
        postgres_conn.execute(
            "INSERT INTO meter VALUES (%s, %s)", [meter.meter_id, meter.user_id]
        )

    def insert_read(read: HalfHourlyMeterRead) -> None:
        postgres_conn.execute(
            "INSERT INTO read VALUES (%s, %s, %s)",
            [read.meter_id, read.timestamp, read.value],
        )

    insert_user(user_1)
    insert_meter(meter_1)
    for read in half_hourly_reads_1:
        insert_read(read)

    n = int(request.config.getoption("--n-profile"))

    for i in range(n):
        insert_user(User(user_id=i + 10, name=f"user-{i + 10}"))
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

    postgres_conn.commit()

    with cProfile.Profile() as pr:
        rows = list(postgres_conn.execute(qry))

    pr.dump_stats("test_classic.prof")

    print("\ntest_classic took:")
    print_time(pr)
    pprint(rows[:3])

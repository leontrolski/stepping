from textwrap import dedent
from datetime import date, datetime
from typing import Any
from uuid import UUID

import stepping as st


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


class UserMeterRead(User, Meter, HalfHourlyMeterRead):
    date: date


class DailyUsage(st.Data):
    user_id: int
    meter_id: UUID
    date: date
    value: float


def make_user_meter(p: st.Pair[User, Meter]) -> UserMeter:
    return UserMeter(
        user_id=p.left.user_id,
        name=p.left.name,
        meter_id=p.right.meter_id,
    )


def with_date(p: st.Pair[UserMeter, HalfHourlyMeterRead]) -> UserMeterRead:
    return UserMeterRead(
        user_id=p.left.user_id,
        name=p.left.name,
        meter_id=p.left.meter_id,
        timestamp=p.right.timestamp,
        value=p.right.value,
        date=p.right.timestamp.date(),
    )


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

# reference: query
index_daily = st.pick_index(DailyUsage, lambda d: d.date)
daily_cache = st.Cache[DailyUsage]()


def query(
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
    grouped = st.group_reduce_flatten(
        merged,
        by=st.pick_index(UserMeterRead, lambda f: (f.user_id, f.meter_id, f.date)),
        zero=float,
        pick_value=pick_value
    )
    as_daily = st.map(grouped, f=to_daily)

    _ = daily_cache[as_daily](lambda a: st.integrate_indexed(a, indexes=(index_daily,)))

    return as_daily
# /reference: query

# reference: data
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
# /reference: data


def test_profile_1(postgres_conn: st.ConnPostgres) -> None:
    # reference: insert
    graph = st.compile(query)
    store = st.StorePostgres.from_graph(postgres_conn, graph, create_tables=True)
    i_users, i_meters, i_reads = st.actions(store, graph)
    i_users.insert(user_1)
    i_meters.insert(meter_1)
    i_reads.insert(*half_hourly_reads_1)

    actual = list(
        daily_cache.zset(store).iter_by_index(
            index_daily, frozenset((date(2023, 1, 2), date(2023, 1, 3)))
        )
    )
    expected = [
        (
            date(2023, 1, 2),
            DailyUsage(
                user_id=1,
                meter_id=UUID("00000001-b3fb-47ef-b22f-a45ba5b7b645"),
                date=date(2023, 1, 2),
                value=18.5,
            ),
            1,
        ),
        (
            date(2023, 1, 3),
            DailyUsage(
                user_id=1,
                meter_id=UUID("00000001-b3fb-47ef-b22f-a45ba5b7b645"),
                date=date(2023, 1, 3),
                value=8.0,
            ),
            1,
        ),
    ]
    assert actual == expected
    # /reference: insert

    # reference: remove
    i_reads.remove(half_hourly_reads_1[2])  # remove a read from 2023-01-02
    actual = list(
        daily_cache.zset(store).iter_by_index(
            index_daily, frozenset((date(2023, 1, 2), date(2023, 1, 3)))
        )
    )
    expected = [
        (
            date(2023, 1, 2),
            DailyUsage(
                user_id=1,
                meter_id=UUID("00000001-b3fb-47ef-b22f-a45ba5b7b645"),
                date=date(2023, 1, 2),
                value=6.0,
            ),
            1,
        ),
        (
            date(2023, 1, 3),
            DailyUsage(
                user_id=1,
                meter_id=UUID("00000001-b3fb-47ef-b22f-a45ba5b7b645"),
                date=date(2023, 1, 3),
                value=8.0,
            ),
            1,
        ),
    ]
    assert actual == expected
    # /reference: remove

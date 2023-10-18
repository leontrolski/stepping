---
title: "Meter reads"
description: ""
lead: ""
date: 2020-10-13T15:21:01+02:00
lastmod: 2020-10-13T15:21:01+02:00
draft: false
images: []
menu:
  docs:
    parent: "examples"
weight: 301
toc: true
---

## Problem

We're an energy supplier, users have meters have reads, we'd like to sum up each meter's daily usage.

With `stepping`, we're able to declaratively describe a query joining and grouping data. When we insert new data, it knows only to recompute what is necessary to update the output. When we're done inserting, we can easily retreive data back out from an indexed cache.


## Setup

We're going to skip over most of the setup -- full details in [test_meter_reads.py](https://github.com/leontrolski/stepping/blob/main/docs/snippets/test_meter_reads.py).

For context, it's hopefully enough to know that we have:

```python
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

def make_user_meter(p: st.Pair[User, Meter]) -> UserMeter: ...
def with_date(p: st.Pair[UserMeter, HalfHourlyMeterRead]) -> UserMeterRead: ...
def to_daily(p: st.Pair[float, tuple[int, UUID, date]]) -> DailyUsage: ...
def pick_value(u: UserMeterRead) -> float: ...
```

Here's our test data:

```python [/docs/snippets/test_meter_reads.py::data]
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
```

## Query

Now let's write our query:

```python [/docs/snippets/test_meter_reads.py::query]
index_daily = st.Index.pick(DailyUsage, lambda d: d.date)
daily_cache = st.Cache[DailyUsage]()


def query(
    users: st.ZSet[User],
    meters: st.ZSet[Meter],
    reads: st.ZSet[HalfHourlyMeterRead],
) -> st.ZSet[DailyUsage]:
    join_meters = st.join(
        users,
        meters,
        on_left=st.Index.pick(User, lambda u: u.user_id),
        on_right=st.Index.pick(Meter, lambda m: m.user_id),
    )
    join_meters_flat = st.map(join_meters, f=make_user_meter)
    join_reads = st.join(
        join_meters_flat,
        reads,
        on_left=st.Index.pick(UserMeter, lambda p: p.meter_id),
        on_right=st.Index.pick(HalfHourlyMeterRead, lambda m: m.meter_id),
    )
    merged = st.map(join_reads, f=with_date)
    grouped = st.group_reduce_flatten(
        merged,
        by=st.Index.pick(UserMeterRead, lambda f: (f.user_id, f.meter_id, f.date)),
        zero=float,
        pick_value=pick_value
    )
    as_daily = st.map(grouped, f=to_daily)

    _ = daily_cache[as_daily](lambda a: st.integrate_indexed(a, indexes=(index_daily,)))

    return as_daily
```

Before querying, we set up a cache of the `DailyUsage`, indexed by date.

In the query itself, we join users to meters to reads.

Then we add the date of each read (from its `.timestamp`).

Then we group by user_id, meter_id, date, and convert to `DailyUsage`. This gets stored in the cache.

## Insert

Now lets insert some data, then assert that the values we retrieve form the cache make sense:

```python [/docs/snippets/test_meter_reads.py::insert]
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
```

## Remove

Remember, we can set up a store any time from any process by doing:

```python
graph = st.compile(query)
store = st.StorePostgres.from_graph(postgres_conn, graph, create_tables=False)
```

Now let's remove a read, and make sure that the daily value for `2023-01-02` has gone down.

```python [/docs/snippets/test_meter_reads.py::remove]
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
```

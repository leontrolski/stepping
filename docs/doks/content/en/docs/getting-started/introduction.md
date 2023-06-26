---
title: "Introduction"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "getting-started"
weight: 101
toc: true
---

You can kinda think of `stepping` as "React for backends". 

In ye olde JQuery days, you would:

- Render the page.
- Set up a load of listeners that [twiddled](https://stackoverflow.com/questions/11189136/fire-oninput-event-with-jquery) with the elements on the rest of the page.

This often turned into a spaghetti mess, as a change of input might need to update many other components and you had competing callbacks happening asynchronously. 

React (in theory at least) solved this by allowing you to do:

- Render the page as `f(state)`
- `<input onInput=mutateState(...) >`
- Re-render the page by recomputing `f(state)`

**Deciding which bits of the page to twiddle is [taken care of](https://leontrolski.github.io/33-line-react.html) by React**. 

Using `stepping` involves a similar shift, but on the backend, in this case from twiddling with cached data in the database to declaratively describing `outputs = f(inputs)` and letting `stepping` handle efficient updates. 

![Diagram](/images/stepping-frontpage.svg)

## Why?

The Python backend you're currently building probably has a really simple "interview-question" version along the lines of:

```python
test-data/
    external-api-call-2012-01-01.json
    user-input-2012-01-02.json
    ...

def process_data(
    inputs: list[Input],
    t: int
) -> Output:

    output = f(inputs[:t])
    
    return output
```

Where `t` is time and the `Output` is the state of the system considering _all the inputs up to and including that time_.

There's many reasons why your production system has more complexity -- often, computing `process_data(...)` at request-time would be prohibitively expensive -- if you squint, a lot of backend code exists to surmount this problem by writing to various caches. 

`stepping`'s aim is to try and let you write your production system something closer to `process_data(...)` -- you describe a rich, declarative function of all your inputs, feed it changes, and it tells you what changed in the output.

There are some example applications [here]({{< ref "/docs/examples" >}} "Example applications").


## Incremental View Maintenance

In most SQL dbs there are two ways of declaratively describing `outputs = f(inputs)`, each with different pros and cons:

- [`VIEW`s](https://www.postgresql.org/docs/current/sql-createview.html) -- the output is always up to date, but can be slow to `SELECT` from as the data has to be recomputed each query.
- [`MATERIALIZED VIEW`s](https://www.postgresql.org/docs/current/rules-materializedviews.html) -- `SELECT` is quick because the data has been precomputed, but the data is only as fresh as the most recent `REFRESH MATERIALIZED VIEW` (which might itself be an expensive operation).

[Incremental View Maintenance](https://scholar.google.com/scholar?hl=en&as_sdt=0%2C5&q=incremental+view+maintenance&btnG=) is an attempt to have one's cake and eat it - **fresh data, quickly**. `stepping` is built on the work described in the paper [DBSP: Automatic Incremental View Maintenance for Rich Query Languages](https://arxiv.org/pdf/2203.16684.pdf).

### Existing Incremental View Maintenance software

There are numerous existing pieces of Incremental View Maintenance software, notably:

- [Materialize](https://materialize.com/)
- [Apache Flink](https://flink.apache.org/)
- [ksqlDB](https://ksqldb.io/)
- [kafka streams](https://kafka.apache.org/documentation/streams/)


_Jamie Brandon has written a nice [taxonomy](https://www.scattered-thoughts.net/writing/an-opinionated-map-of-incremental-and-streaming-systems/) of them._


### Then why write stepping?

The niche `stepping` tries to sit in is:

- Less focus on big-data pipelines, more focus on application development.
- Allows describing the computation in Python not SQL.
- Can sit next to existing applications, potentially sharing Postgres databases/transactions.
- Provide an educational example of [DBSP](https://arxiv.org/pdf/2203.16684.pdf) - about 3000 lines of pure Python at time of writing.

## What about Event Sourcing?

Event Sourcing has many meanings depending on who you speak to. For example: the classic Martin Fowler [definition](https://martinfowler.com/eaaDev/EventSourcing.html), Martin Kleppmann's influential [talk](https://www.confluent.io/en-gb/blog/turning-the-database-inside-out-with-apache-samza/).

In practice, these systems often amount to many services broadcasting changes to each other over message buses. This can lead to a some problems:

- The [developer ergonomics](https://leontrolski.github.io/cmd-click-manifesto.html) can be bad.
- Replaying messages on changes to the code (and the downstream ramifications) are often an afterthought. (`stepping` will in future try to tackle this in an opinionated manner with [stepping manager]({{< ref "/docs/stepping-manager/managing-live-systems.md" >}} "Stepping manager")).
- No `TRANSACTION`s.
- Often no easy way to express `JOIN`s/`GROUP BY`s.
- Shifting all the messages around over the wire often incurs significant performance cost -- see below.

## Should I use stepping?

Probably not, at least right now:

- For most applications, storing all the data in normalised form with suitable indexes, then computing everything at request-time in a single thread will outperform `stepping` (or any other Event Sourcing approach for that matter). [Profile](https://jiffyclub.github.io/snakeviz/) your code!
- `stepping` is currently very much in Alpha form.

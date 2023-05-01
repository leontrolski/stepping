
## _Build your backends declaratively._

### Why?

Consider the interview-question version of the Python backend you're currently building. It probably looks along the lines of:

```
data/input.json

def process_data(input) -> Output:
    intermediary_value = f(input)
    ...                = g(intermediary_value)
    ...
    output             = l(...)
    return output
```

It takes someone an afternoon to write it. Meanwhile, the breakdown of time spent building your production backend looks like:

- X% Writing business logic.
- X% Cache gubbins - maintaining caches of data (this includes not only "classic" caching using Redis/Memcached, but _everywhere_ where you write denormalised data for performance reasons).
- X% Database gubbins - interacting with SQL, marshalling data, migrating data, etc.
- X% REST gubbins - interacting with external systems/other services, marshalling cross-service types, etc.
- X% Queue gubbins - ditto, but with some queue service (think rabbitmq/pubsub etc).

_Look back over Bulb presentation._

`stepping` aims to reduce the amount of gubbins involved in building software, leaving you more time to focus on business logic.


### Folded section "Shifting to declarative"

_Like wot React did for the frontend/terraform does for infrastructure._


### Properties of `stepping`

- Orthogonal - Fits in right next to your existing software - no need for rewrites.
- Declarative - The magic of **Incremental View Maintenance** lets you describe outputs as a function of inputs without error-prone update logic.
- Performant - Automatically caches output data with indexes for high read throughput.
- Reliable - Uses `Postgres` to store data, this allows use of existing tooling for backups, monitoring etc.
- Faff free - UIs and opinionated CLI tools reduce dev-ops burden.
- Well typed - Uses the latest `Python` typing features to aid development.

### Example

_Spell out one of the following:_

- Further expand on the energy industry meter reads example, it's actually a pretty good match.
- Dashboard-like pages with graphs per day over time.
- Build system a la Bazel.
- Compiler.
- UI.
- Anything with a comments page, demonstrate what happens if we dump the `<html>` on changes.
- Realtime interactive page like Google docs.
- A bank-like thing with money coming in and out?
- All the scooters in a square mile - think Lime.
- The Twitter design question - think about `WHERE is_celeb(user)`. Too complicated?


## Page layout

_The opening page should be flashy, like https://godotengine.org_

```
why?        |
properties  |  some writing
how?        |  ...
example     |
------------|
dev docs    |
```

## Todo

### High level ideas

- Implement something more complicated from the examples above.
- Problems with turning the db inside out - see previous wip, error handling, replaying new functions/structures, impure API calls to, schema migration.
- A frontend version? Probably not worth thinking about at this point.

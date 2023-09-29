---
title: "Known Caveats"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "in-production"
weight: 608
toc: true
---

`stepping` is very much alpha software, here are a few gotchas:

- In general, it's not battle hardened, I'm still finding many bugs during development.
- There are some fragile bits of code, particularly `compile(...)`, which does some crazy AST based stuff.
- Storing all the delay nodes takes up lots of space (I'm not sure how/if e.g. materialize gets round this) -- picking a different serialization method should help a lot -- see [performance]({{< ref "/docs/in-production/performance.md" >}}).
- The big O performance seems reasonable from my benchmarking, but the fixed overhead per iteration is quite long in some cases, again, see [performance]({{< ref "/docs/in-production/performance.md" >}}).
- When a query function returns a `ZSetSQL`, the outputs can be inconsistent because it will give the current (as opposed to the previous) value from the delay vertex -- make sure to use `st.ensure_python_zset(...)` on these outputs.
- It needs a fair bit of work to make parallelizable -- currently only one `st.iteration` can run at a time whilst maintaining consistent data.
- Ordering of JSON `null`s not tested. There are likely bugs in this area (where we map to/from JSON in the db).
- There's nothing to help migrate queries between versions. This is a two fold problem (which [steppingmanager]({{< ref "/docs/in-production/stepping-manager.md" >}}) eventually aims to solve):
  - Vertices' paths are fragile as they are a hash of the modules/function names of each sub-query.
  - Updating the schema of data has no clear migration path for all the data stored in the delay nodes.
- Updating an oft-referenced value causes lots of work to happen all at once (again I'm not sure how/if e.g. materialize gets round this). For example, if a million users all join with some value and you change that value, a `ZSetPython` with a million values will be outputted from the join. Avoiding this requires some amount of foresight when writing queries -- far reaching pieces of data are better joined at final query time.

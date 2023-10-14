---
title: "Stepping Manager"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "in-production"
weight: 606
toc: true
---

_`steppingmanager` may at some point get written._

It's aim is to help with deploying and migrating multiple queries in production.

## Ideas

- A query might get deployed via some Heroku like mechanism, with persistence, a REST API, client libraries, etc. "for free".
- Deployed `stepping`s should have persistence at the input and the output to allow for replaying data on changes. It should be easy to eg. replay a load of `POST`s between instances.
- A `/steppingmanager` directory should know about the graph of deployments, and handle replays etc.
- We should version control a dump of the whole graph. This should be a `.py` file, but with no dataclasses, only atoms (int, str, etc) - see `isjsonschemasubset`.
- Handle schema changes by replaying data/connecting to old sources.
- A worker should expose the diff between the time of the most recent event and most recently published, or some vaguely Spanner-like approach to consistency.
- There should be some graceful error handling for queries.

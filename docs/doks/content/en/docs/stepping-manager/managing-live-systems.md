---
title: "Managing live systems"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "stepping-manager"
weight: 402
toc: true
---

_Coming soon..._

### _Nice diagram here_

## Ideas

- Deployed `stepping`s should have persistence at the input and the output to allow for replaying data on changes. It should be easy to eg. replay a load of `POST`s between instances.
- A `/steppingmanager` directory should know about the graph of deployments, and handle replays etc.
- They should generate FastAPI apis, and have `Python` library interfaces for RPC-like calls, with nice typing.
- We should version control a dump of the whole graph. This should be a `.py` file, but with no dataclasses, only atoms (int, str, etc).
- Handle schema changes by replaying data/connecting to old sources.
- For horizontal scaling, think about using `PREPARE TRANSACTION`.
- A worker should expose the diff between the time of the most recent event and most recently published.

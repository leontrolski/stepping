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

`stepping` is a Python **Incremental View Maintenance** library, conceptually similar to [Materialize](https://materialize.com/), but with a focus on application developers as opposed to big data wranglers. 

For what that means exactly, have a read through the [motivations]({{< ref "/docs/internals/motivation.md" >}}). 

`stepping` is built on the work described in the paper [DBSP: Automatic Incremental View Maintenance for Rich Query Languages](https://arxiv.org/pdf/2203.16684.pdf). **It's currently in a very alpha state** (see [caveats]({{< ref "/docs/in-production/caveats.md" >}})), so probably more useful as a small [reference]({{< ref "/docs/internals/how-it-works.md" >}}) implementation of DBSP than something you might want to use for real work™.

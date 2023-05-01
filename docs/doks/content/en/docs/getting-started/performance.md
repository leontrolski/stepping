---
title: "Performance"
description: ""
lead: ""
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: []
menu:
  docs:
    parent: "getting-started"
weight: 110
toc: true
---

_Example with numbers, things to bear in mind._

- Running a pretty basic test (1 million reads, two joins, group by date), stepping's insert time is a lot slower, but querying the integrated data set takes `0.0003s` as opposed to `0.5s`. (Details in `test_profile_cute.py`).

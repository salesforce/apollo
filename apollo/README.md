# Apollo

This is the core Apollo service.  The Apollo service is the full integrated Apollo stack, runnable as a single process, or as an embedded library.  It is designed such that one can easily configure Apollo via simple YAML.  This module produces a [Maven Shaded](https://maven.apache.org/plugins/maven-shade-plugin/examples/executable-jar.html), execuable, phat jar, which is a dependency free, stand alone Jar artifact for the Apollo stack.

## Status
This is the main target of this project, and works pretty well.  It's functionally complete, modulo the individual subsystems of Apollo, which are still in flux.

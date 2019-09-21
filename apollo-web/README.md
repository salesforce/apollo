# Apollo Web
This module is a [Dropwizard](https://www.dropwizard.io/en/stable/) embedding of the Apollo Service.  The Apollo service is exposed through a number of REST Apis, using Dropwizard and JaxRS.  This module produces a [Maven Shaded](https://maven.apache.org/plugins/maven-shade-plugin/examples/executable-jar.html), execuable, phat jar, which is a dependency free, stand alone Jar artifact for the Apollo Web stack.

## Status
Dropwizard is teh easy and provides a real world example of running an accessible Apollo service node.  However, this iteration of Apollo web has a paltry selection of APIs at the moment, providing a bare minimum of functionality for the Avalanche layer only.  Ghost is not exposed in this module.

# Apollo Boostrap Service

This is a bare bones web service for bootstrapping Apollo service nodes.  This service provides a persistent management of membership for the Apollo network.  Apollo requires a central certificate authority that manages both the shared trusted signing of member node certicates (this PKI is at the heart of Fireflies), but also the membership ids required by the Fireflies Node.

Apollo was designed to run on cloud infrastructure, and as such has to deal with issues like DHCP - where we don't know the IP address of the node until it is instantiated - as well as the reality of Loadbalancers and other network in/egress required by modern VPC systems.  The bootstrap service is, in effect, simply a certificate signing service, and so should be able to be "swapped out" for such.  However, Fireflies relies on the member node's ID as well as the parameters for defining the Fireflies rings (byzantine tolerance, etc) in the certificate itself.  Thus, the certificate signing request satisfaction is a group membership tracking service that also happens to be the trusted CA.

## Status
POC quality only.  Reasonably tested.

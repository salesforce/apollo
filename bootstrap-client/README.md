# Apollo Bootstrap Client

This is the embeddable REST client used to interact with the Apollo bootstrap service.  This service generates the private key used by the node and sends off the equivalent of a certificate signing request to the bootstrap service via HTTP POST.  The result is a Java keystore containing the signed certicate for the Node's generated key, which embeds the Fireflies parameters an Apollo service <host:port>'s the node uses for secure communications.

## Status
Works well and is quite secure. No auth on the request, and uses HTTP, though.  So, POC quality only.

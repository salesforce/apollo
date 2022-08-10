# Permissive Action Link Client
PAL is a tool for provisioning secrets to processes in production.
## Usage
The PAL Client is provisioned with encrypted secrets (ciphertext) which the client passes to the PAL Daemon for decryption.  These secrets are supplied to the process/container through provisioning.  Upon startup or when required, the client process passes these secrets to the PAL Daemon using a GRPC connection over Unix Domain Sockets.  The response, if the client is authorized, is the decrypted form of the supplied secrets.

## Architecture
Please see the [PAL Daemon](../pal-d/README.md) for architecture.

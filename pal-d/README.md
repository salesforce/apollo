# Permissive Action Link Daemon
PAL is a tool for provisioning secrets to processes in production.

## Architecture
The PAL architecture uses a client/server pattern.  The PAL Client runs as the entrypoint in a container or process and the PAL Daemon, that runs outside the container or process on the same machine.  The PAL Daemon accepts requests from PAL clients using GRPC over unix domain sockets.  The PAL Daemon uses the Peer Credentials provided by the client's domain socket to authenticate the client and determine what secrets the client is authorized for.

The PAL Client is provisioned with encrypted secrets (ciphertext) which the client passes to the PAL Daemon for decryption.
## Authorization
The PAL Daemon uses simple String labels for determining authorization.  The PAL Daemon uses a supplied function in construction to retrieve the list of valid (authorized) labels for a give client process.  This function takes the PeerCredentials of the connecting client (Process ID, User ID, List<Group ID>) and returns the list of authroized labels.
## Decryption Backends
The PAL Daemon is a completely generalized service that uses other services (or code) to provide the decryption of client secrets.  Each decryption service has an associated String label and a Map of these labels to functions that decrypt these secrets is supplied to the PAL Daemon.  This completely decouples the PAL Daemon from the implementation of the service backends required.
## Configuration
The PAL Daemon is simply configured by its constructor, which takes a Path representing the Unix Domain Socket the daemon listens on, the Function that supplies the list of valid labels based on the supplied PeerCredentials, and a Map of decryption service functions that provide the decryption of the supplied secrets.
## GRPC
The PAL Daemon is a GRPC service and can be accessed by any GRPC client via the Unix Domain Socket configured for the PAL Daemon.  The service has a simple, single API: __decrpypt__  with the __Encrypted_  secrets and returns the result in the __Decrypted_ response.

The __Encrypted__ secrets are a Map of Strings that represent the "environment" to the __Secret__  supplied for that environment label.  The __Secret__  consists of a List of String labels required for the secret, a String that labels the decryption service to use for this secret, and finally the arbitrary bytes themselves that form the secret to decrypt.

The __Decrypted__ secrets are simply a Map of the same String "environment" labels with the corresponding decrypted bytes.  If there are errors, these are returned in the __errors__ field of the __Decrypted__ response.
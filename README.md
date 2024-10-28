# ProxyGuard

Proxy UDP connections over HTTP(s). The main use case is to proxy WireGuard packets.

# Goal

The goal of this project is NOT to work around _intentional_
state/organizational network blocks/censorship. We developed ProxyGuard to work
on networks that are misconfigured. For example on networks where UDP is
blocked, or there is an issue with the MTU. Therefore, ProxyGuard currently
does not do any advanced anti-censorship tricks, it is merely a proxy for UDP
traffic. This keeps the codebase simple.

# Dependencies

- Go (target: >= 1.19)

# Building

Run: `make`

> **_NOTE:_**  You can build static binaries with `CGO_ENABLED=0 make`

# Installing

Run:

```bash
$ sudo make install
```

This will install the daemons under `/usr/local` and add the `systemd` 
service files. Do not forget to run `systemctl daemon-reload`. After that you
can start the `proxyguard-server` and `proxyguard-client` services.

To configure them, use e.g. `systemctl edit proxyguard-server` and override the
variables you see there with your own values as needed, for example if 
WireGuard is listening on port 443, you can add the following:

```ini
[Service]
Environment=TO=127.0.0.1:443
```

## DEB/RPM packages

Currently there are DEB/RPM packages available in eduVPN's server repository, [follow the eduVPN docs to add the repo](https://docs.eduvpn.org/server/v3/repo.html).

And then install ProxyGuard using: `sudo apt -y install proxyguard-client` or `sudo dnf -y install proxyguard-client` 

# Running
This tool is focused on a client-server model. This proxy thus needs to run for every client and for a server. The server mode accepts multiple clients.

## Client example

This example listens on local UDP port 51821, expects packets from UDP port 51820 (WireGuard listen port) and forwards TCP packets (with a HTTP Upgrade handshake) to vpn.example.com

```bash
proxyguard-client --to http://vpn.example.com
```

> **_NOTE:_**  If you test the client on Linux, you might also need to add --fwmark 51820 (or some other number that the WG connection configures) to mark that the packets going out of this proxy are encrypted, preventing a routing loop. 51820 is the default for WireGuard. Note that this needs root or the binary needs `CAP_NET_ADMIN`.

> **_NOTE:_**  In case HTTPS is used, the client only allows servers with TLS >= 1.3

To then use this with WireGuard, you need to change the endpoint in the WireGuard config and make sure the listen port is set

```ini
[Interface]
PrivateKey = ...
Address = ...
DNS = ...
ListenPort = 51820

[Peer]
PublicKey = ...
AllowedIPs = ...
Endpoint = 127.0.0.1:51821
```

## Server example

This example starts a HTTP server on TCP port 51821 and forwards UDP packets to localhost 51820, the WireGuard port.

```bash
proxyguard-server --listen 0.0.0.0:51821 --to 127.0.0.1:51820
```

# Deployment
For details on deployment, [see here](./deploy.md).

# Technical docs
For technical docs, [see here](./technical.md).

# Acknowledgements & Thanks

The following projects gave me an idea how I could implement this best, most notably how to do the UDP <-> TCP conversion with a custom header. This project started with a UDP to TCP approach, but later moved to HTTP for use behind a reverse proxy and improved obfuscation.
- https://github.com/mullvad/udp-over-tcp
- https://github.com/rfc1036/udptunnel
- https://nhooyr.io/websocket

# License
[MIT](./LICENSE)

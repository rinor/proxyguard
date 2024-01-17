# Proxyguard

Proxy WireGuard UDP connections over TCP

# Dependencies

- Go (target: >= 1.19)

# Building

Run: `make`

> **_NOTE:_**  You can build static binaries with `CGO_ENABLED=0 make`

# Running
This tool is focused on a client-server model. This proxy thus needs to run for every client and for a server. The server mode accepts multiple clients.

## Client example

This example listens on local UDP port 1337 and forwards TCP packets to vpn.example.com 1337

```bash
proxyguard-client --listen 127.0.0.1:1337 --to vpn.example.com:1337
```

> **_NOTE:_**  If you test the client on Linux, you might also need to add --fwmark 51820 (or some other number that the WG connection configures) to mark that the packets going out of this proxy are encrypted, preventing a routing loop. 51820 is the default for WireGuard. Note that this needs root.

The received TCP packets from the server (in this case `vpn.example.com`) are forwarded back to the address of the first received UDP packet. So if the proxy receives an UDP packet from port x first, it will remember this as the destination for received packets. This is so that WireGuard can use a dynamic port.


To then use this with WireGuard, you need to change the endpoint in the WireGuard config:

```ini
[Peer]
PublicKey = pubkeyhere
AllowedIPs = 0.0.0.0/0, ::0
Endpoint = 127.0.0.1:1337
```

## Server example

This example listens on TCP port 1337 and forwards UDP packets to localhost 51820, the WireGuard port.

```bash
proxyguard-server --listen 0.0.0.0:1337 --to 127.0.0.1:51820
```

# Acknowledgements & Thanks

The following projects gave me an idea how I could implement this best, most notably how to do the UDP <-> TCP conversion with a custom header:
- https://github.com/mullvad/udp-over-tcp
- https://github.com/rfc1036/udptunnel

# License
[MIT](./LICENSE)

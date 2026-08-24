# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
# Бинарь собирается локально (`cargo deb -o dist`) и доставляется .deb-пакетом —
# внутри Docker компиляции нет. Хост и образ — Debian 13 (trixie, glibc 2.41),
# поэтому deb, собранный на хосте, ставится чисто.
FROM debian:trixie-slim

COPY dist/swarm-mcp_*.deb /tmp/swarm-mcp.deb
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && dpkg -i /tmp/swarm-mcp.deb \
    && rm -f /tmp/swarm-mcp.deb \
    && rm -rf /var/lib/apt/lists/*

USER 1000:1000
EXPOSE 3004

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/swarm-mcp", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/swarm-mcp"]
CMD ["serve"]

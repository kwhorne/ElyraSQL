# Security Policy

## Supported versions

ElyraSQL is pre-1.0 and under active development. Security fixes target the
latest release and `main`.

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅ |
| < 0.1   | ❌ |

## Reporting a vulnerability

**Please do not open a public issue for security vulnerabilities.**

Report privately via GitHub Security Advisories:
<https://github.com/kwhorne/ElyraSQL/security/advisories/new>

Include, where possible:

- A description of the issue and its impact.
- Steps to reproduce (schema, statements, configuration).
- Affected version(s) and platform.

We aim to acknowledge reports promptly and will coordinate a fix and disclosure
timeline with you.

## Hardening

Operational guidance for running ElyraSQL safely (authentication, TLS, roles,
network exposure) is in the
[Security documentation](https://kwhorne.github.io/ElyraSQL/security/).

!!! note
    Running with no credentials ("open mode") is intended for local development
    only and logs a warning. Always configure authentication and TLS before
    exposing the server.

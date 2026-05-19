# Auth FAQ

Short answers to the questions that come up when you read DUNE / dunecat
auth docs and trip over the jargon. Companion to
[`auth-presentation.md`](auth-presentation.md) (slide-form telling of the
same story) and to the operational docs
[`auth.md`](auth.md) and [`hub.md`](hub.md).

## The basics

### What is the difference between *authentication* and *authorization*?

- **Authentication** = "who are you?"
- **Authorization** = "what are you allowed to do?"

A bearer token answers both at once: it proves identity *and* carries
the permissions the issuer granted. Most documentation (this one
included) uses "auth" loosely to mean both.

### What is a credential?

Anything you can hand to a server as proof of identity. A password, a
private key + certificate pair, a bearer token, even an SSH key. The
schemes we'll see differ in *what form* the credential takes.

### What is a token?

A piece of data — usually a string — that proves something to a
server. A bearer token proves "the holder is authorised." A refresh
token proves "the holder may mint new access tokens." A session token
proves "the holder owns this browser session."

The word *token* is generic. Always look at what kind of token is in
play.

## Passwords

### Why are passwords considered insecure?

Three reasons:

1. **Reuse.** Users reuse them across sites. A breach at any one site
   compromises all the others.
2. **Phishability.** A convincing fake login page captures the secret
   in plaintext.
3. **No built-in expiry.** A leaked password keeps working until
   someone manually changes it.

Modern systems prefer credentials that expire automatically and don't
have to be retyped (or memorised) at all.

### Does dunecat use passwords?

Only as a fallback for metacat:

```bash
uv run dunecat login metacat --method password
```

The default flow is OIDC. The password path stays around because
sometimes the OIDC server-side configuration breaks and we need a way
in while operators fix it.

## Certificates and keys

### What is a private key?

A long random number — typically RSA, EC, or Ed25519 — that you keep
secret. Anything signed with this key can be verified by anyone who
has the matching *public* key, but only you can produce the
signatures. That asymmetry is the basis of all modern public-key
cryptography.

### What is a certificate?

A small file that bundles together: (1) your public key, (2) some
identity claims about you ("this key belongs to chao.zh@bnl.gov"),
and (3) a signature from a Certificate Authority the server trusts.

Think of it as an ID badge: it identifies you, but you only get it
because a trusted issuer vouches for you.

### What is the difference between a "cert" and a "key"?

The certificate (`.crt`, `.pem`) is the *public* half — it identifies
you to anyone who looks. The key (`.key`) is the *private* half —
only you have it, and it's how you actually prove the certificate is
yours. They always come as a pair.

If you ever see a `.p12` or `.pfx` file, that's both halves packed
into a single file with a password on it.

### What is X.509?

The most common standard format for certificates — the format you'll
see used by HTTPS, by SSL, by the grid, and by DUNE's older auth
systems. When people say "X.509" they usually mean "certificate-based
auth."

### Does dunecat use X.509?

No. dunecat's auth path is entirely OIDC. You may still see X.509
artefacts (`~/.globus/`, proxy files, `voms-proxy-init`) on shared DUNE
machines because the grid still uses them, but they don't enter
dunecat's flow.

## OAuth and OIDC

### What is OAuth 2.0?

A protocol for one program to act on behalf of a user at another
service, *without* the user handing over their password. The output
of an OAuth flow is an **access token**: a piece of paper saying
"the holder may do these specific things at this specific service
until time X."

OAuth alone does not say *who* the user is — only what the bearer is
permitted to do.

### What is OIDC?

**OIDC = OAuth 2.0 + identity.** It re-uses OAuth's flow but adds an
**ID token** whose only job is to tell the receiver who the user is.
The ID token is a JWT containing standard claims like `sub` (a
stable identifier), `email`, `iss` (who issued it), and `exp` (when
it expires).

OIDC is what powers virtually every "Sign in with Google / GitHub /
Microsoft" button on the web.

### What is "Sign in with X" doing under the hood?

It's an OIDC authorization-code flow. Your browser is redirected to
X's login page; after you log in there, X redirects you back to the
original site with a one-time code; the site exchanges that code
server-side for an access token and an ID token. The ID token tells
the site who you are.

## Tokens — bearer, JWT, opaque

### What is a bearer token?

A token that authorises whoever holds it, with no further proof
required. It's sent in an HTTP header:

```
Authorization: Bearer eyJhbGciOi...
```

The word "bearer" means: the server does not check *whose* token
this is, only that the token is valid. Like a hotel keycard — drop
it in the hall and the next person who picks it up gets into your
room.

### Why is that not terrifying?

Three mitigations:

1. **Always HTTPS.** A bearer token must never travel in cleartext.
2. **Short lifetimes.** Typically 1–6 hours. If a token leaks, it's
   only useful until it expires.
3. **Restricted audience.** A JWT has an `aud` claim — most servers
   reject tokens not meant for them.

### What is a JWT?

A token format. Three base64-url chunks separated by dots:

```
eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJjaGFvIiwiZXhwIjoxNzAwMDAwMDAwfQ.SflKxw...
   ↑ header             ↑ payload                                ↑ signature
```

The payload is just JSON, readable to anyone — it's *signed*, not
encrypted. A receiver with the issuer's public key can verify the
signature offline and trust the payload.

Paste any JWT into <https://jwt.io> to see the JSON inside.

### Is a JWT encrypted? Can I read it?

It is **signed, not encrypted** (by default). Anyone who has the
token can read the payload by base64-decoding it. Never put secrets
inside a JWT payload, and treat the whole JWT as confidential when
sending it.

### What is an "opaque" token?

A random string with no internal structure — e.g. `d8f9a7b…`. To
know what it means, the receiver must look it up in the issuer's
database. The opposite of a JWT, which is self-describing.

Trade-off:
- JWT: cheap to verify (offline), hard to revoke (valid until expiry).
- Opaque: revocable instantly (delete the row), but every check costs
  a database lookup.

### Does dunecat use JWTs or opaque tokens?

Both:
- The **OIDC bearer** from vault is a **JWT** — metacat and Rucio
  verify it offline.
- The **hub session cookie** is **opaque** — a random ID that maps
  to a row in the hub's `sessions` table.

### What is the difference between an *access token* and a *refresh token*?

- **Access token:** short-lived (typically hours). Sent on every API
  call. If it leaks, the damage is limited because it expires soon.
- **Refresh token:** long-lived (days to weeks). Held by a trusted
  client only. Its only job is to mint new access tokens.

Why two? Tension between security (you want what's on the wire to be
short) and UX (you don't want to log in via browser every hour).

### What is a "vault token"?

That's Fermilab-specific. The FNAL `htvaultprod` server stores your
**refresh token** behind a vault-issued opaque token; in DUNE
parlance the "vault token" is the long-lived credential that lets a
client ask vault for fresh bearer JWTs. Lifetime: 10 to 28 days
depending on how it was minted.

## DUNE specifics

### What is CILogon?

A federated identity broker. It lets you "sign in with" your home
institution — your university, lab, Google account, etc. — instead of
making DUNE manage user passwords directly. CILogon is the front
door of DUNE's OIDC flow.

### What is FNAL IAM?

The Fermilab identity management system. It maps your CILogon
identity (which might be "chao@bnl.gov") to a DUNE account and
issues OIDC tokens with DUNE-specific claims.

### What is htvaultprod / vault?

`htvaultprod.fnal.gov` is Fermilab's instance of HashiCorp Vault,
configured to act as an OIDC client and refresh-token store. After
you complete the device-code login once, vault holds a refresh token
for you for 10–28 days; clients (htgettoken, the hub backend) call
vault to mint a fresh bearer JWT each time they need one.

### What is htgettoken?

A small CLI shipped by Fermilab that drives the vault device-code
flow and writes the resulting bearer JWT to `/tmp/bt_u<uid>`. It is
what `dunecat login` calls on your behalf for the local single-user
app. The hub backend skips htgettoken and talks to vault over HTTP
directly.

### What is the device-code flow?

The OIDC flow used by CLIs and headless servers that can't host a
browser themselves. The program shows you a short code and a URL,
you visit the URL in any browser, log in normally, and the program
polls the identity provider until you're done.

Both `dunecat login` and `/hub/login` use device-code flow against
`htvaultprod.fnal.gov`.

### Why does dunecat ask me to log in every ~10 days (or ~28 days in the hub)?

That is the lifetime of your **vault refresh token**. The 3-hour
bearers are minted from it automatically without your involvement,
but once the refresh itself rolls, you have to do the browser device
flow again. The local app uses a 10-day refresh; the hub uses 28 days
(the maximum vault permits).

### Why does dunecat have *two* credential files (`/tmp/bt_u<uid>` and `~/.token_library`)?

`/tmp/bt_u<uid>` is the OIDC bearer JWT from vault. `~/.token_library`
is a *metacat session token* — metacat predates OIDC and keeps its
own session model. When you run `metacat auth login -m token`, metacat
verifies your bearer once and gives you back its own session JWT for
subsequent calls. The session inherits the bearer's expiry, so neither
can outlive the other.

### Why does the hub *not* use `/tmp/bt_*` or `~/.token_library`?

Because it's multi-user. Files on disk in the operator's home or
`/tmp` would either be shared between all users or impossible to
isolate. The hub holds an encrypted vault token per user in SQLite,
mints bearers in memory per-request, and never writes credentials to
disk.

### Is it safe to store the vault token in SQLite on the hub?

The token is encrypted with AES-GCM using a key that lives at
`~/.dunecat/hub.key` (or in `DUNECAT_HUB_SECRET_KEY` in production).
If an attacker steals the SQLite file alone, the ciphertext is
useless. The threat model assumes the key and the DB are stored
separately (different backups, different access controls).

## Web sessions

### What is a session cookie?

A cookie set by the server that ties a browser to a server-side
session. Usually opaque — a random ID — that maps to a row in a
sessions table. It's what makes "stay logged in for a week" work
without storing your password.

### How is a session cookie different from a bearer token?

Cookies are sent automatically by the browser; bearer tokens are
sent explicitly by application code. Cookies live in browser-managed
storage; bearer tokens live wherever the JavaScript or CLI stashes
them. In dunecat hub, the *browser* holds an opaque session cookie;
the *backend* holds the bearer JWTs in memory only.

### What does "HttpOnly" mean on a cookie?

The cookie is invisible to JavaScript — only the browser itself
sends it on HTTP requests. This defeats most XSS attempts to steal
the session. The hub session cookie is HttpOnly.

### What about "Secure"?

The cookie is only sent over HTTPS. The hub sets this when it sees
`X-Forwarded-Proto: https` from the reverse proxy.

## Errors and remediation

### What does "Vault token expired" mean?

Your refresh token (10 days locally, 28 days in the hub) has rolled.
You need to do the browser device-code flow again. Locally that's
`uv run dunecat login`; in the hub it's clicking "Sign in" on
`/hub/login`.

### What does "Token missing or expired" from metacat mean?

Either the bearer JWT you sent is past its `exp` claim, or metacat
couldn't verify it. The local app's web server auto-renews bearers
~5 minutes before expiry; if you're seeing this in the UI it usually
means the vault refresh itself rolled. Run `dunecat login` again.

### What does "Metacat refused OIDC bearer" mean?

Metacat received a valid JWT but rejected it — usually because your
DUNE IAM account isn't provisioned in metacat yet, or there's an
issuer/audience mismatch on the server side. Fall back to password
auth while operators sort it out:

```bash
uv run dunecat login metacat --method password
```

### Why does my bearer have a `sub` claim but no `preferred_username`?

The CILogon-issued JWT only carries a UUID `sub` and some metadata —
it doesn't include the FNAL username directly. The username (e.g.
`chaoz`) is stored separately as vault's `metadata.credkey`, which
the hub records in the `users.metacat_username` column at first
login. Metacat needs that username to call `login_token(username,
bearer)`.

## Further reading

- [`auth-presentation.md`](auth-presentation.md) — same material in
  slide form.
- [`auth.md`](auth.md) — operational guide for the local single-user app.
- [`hub.md`](hub.md) — operational guide for the multi-user hub.
- RFC 6749 — OAuth 2.0.
- OpenID Connect Core 1.0 — the OIDC spec.
- <https://jwt.io> — paste any JWT, see the JSON inside.

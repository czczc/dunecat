# Auth Methods — A Survival Guide

A slide-by-slide walk through the authentication jargon you hit when working
with DUNE services (and dunecat in particular): password, X.509, certificate,
key, bearer, OIDC, JWT, OAuth, vault, refresh token. What each term means,
how they fit together, and which ones dunecat actually uses.

Format: one slide per `---` block. Each slide has a **Visual** section
(what should appear on the slide) and a **Narrative** section (what the
speaker says).

---

## Slide 1 — Title

**Visual:**

> # Auth Methods: A Survival Guide
> Password, certificate, key, bearer, OIDC, JWT — what they are and how they fit together
>
> (with worked examples from dunecat, DUNE metacat, and Rucio)

**Narrative:**

If you've worked with DUNE services for any length of time, you have seen
a wall of words: htgettoken, bearer token, OIDC, JWT, vault, X.509 proxy,
CILogon, FNAL IAM. They all sound interchangeable and they all sound
intimidating. They're not interchangeable, and once you see the picture
they fit into, they stop being intimidating. The goal of the next twenty
minutes is to give you that picture.

---

## Slide 2 — Why this matters

**Visual:**

- Authentication errors are the #1 reason a DUNE tool "just doesn't work"
- The error messages assume you already understand the model
- The model has accumulated three generations of design (password → X.509 → OIDC)
- You need to recognise which generation each tool is using

**Narrative:**

When a tool fails with "401 Unauthorized" or "vault token expired" or
"no proxy certificate found", the message presumes you already know
what kind of credential it wanted. If you don't, the only way forward
is trial and error. So instead of memorising fixes, we will build the
mental model that makes the messages self-explanatory.

---

## Slide 3 — The two questions auth answers

**Visual:**

> Authentication answers two questions:
>
> 1. **Who are you?** (identity)
> 2. **How do you prove it?** (credential)
>
> Every auth scheme is a different answer to question 2.

**Narrative:**

Strip away the jargon and authentication is always doing the same thing:
the server wants to know who is calling, and it wants some proof. The
schemes we'll cover — password, X.509, OIDC, bearer tokens — all answer
the same first question. They differ in how they answer the second.
Once you internalise that, the rest is taxonomy.

---

## Slide 4 — The cast of characters

**Visual:**

| Term            | What it is                                | Layer        |
|-----------------|-------------------------------------------|--------------|
| Password        | A shared secret                           | Credential   |
| Certificate     | A signed public-key + identity bundle     | Credential   |
| Private key     | The secret half of a key pair             | Credential   |
| OAuth 2.0       | Protocol for delegation                   | Protocol     |
| OIDC            | OAuth 2.0 + identity claims               | Protocol     |
| JWT             | A self-describing token format            | Format       |
| Opaque token    | A random ID, meaning is server-side       | Format       |
| Bearer          | "Whoever holds this gets in"              | HTTP scheme  |
| Refresh token   | Long-lived; mints short access tokens     | Role         |
| Access token    | Short-lived; what the server actually checks | Role     |

**Narrative:**

This is the cheat-sheet. Notice the column on the right — these words
live on different *layers*. A bearer JWT access token is not a
contradiction; it's a token that uses the bearer scheme over HTTP,
encoded as a JWT, playing the access-token role in an OAuth 2.0 flow.
We'll spend the rest of the talk peeling those layers apart.

---

## Slide 5 — Layer 1: Password

**Visual:**

```
client ──"user/pass"──▶ server ──compare to hash──▶ ✓ or ✗
```

- **Pro:** trivial to implement, no infrastructure
- **Con:** phishable, reusable across services, no expiry by design,
  server has to store a hash
- **DUNE use:** legacy. `metacat auth login -m password` still works but
  it's a fallback when OIDC misbehaves.

**Narrative:**

Passwords are the oldest answer to "prove who you are": a shared secret
that you and the server both know. Easy to build, awful to scale. They
get reused, they get phished, they don't expire on their own, and a
breach at any one service can put credentials for many services at
risk. In dunecat the password path still exists for metacat
specifically — that's the `--method password` flag — but it is
explicitly a fallback for when the modern OIDC path is broken.

---

## Slide 6 — Layer 2: X.509 certificates and keys

**Visual:**

```
You hold:  private key (secret)  +  certificate (public)
                                     ↑
                          signed by a Certificate Authority
                                  the server trusts
```

- A **certificate** is a public key + identity claims + a CA signature
- A **private key** is the secret half — only you have it
- Together they prove "this party is who the CA says they are"
- **DUNE history:** the grid era used X.509 ubiquitously
  (`voms-proxy-init`, dCache certs, etc.)
- **dunecat use:** none directly, but you may still see `~/.globus/`
  files on shared DUNE machines

**Narrative:**

Cryptographically the certificate model is much stronger than passwords:
no shared secret ever leaves your machine, and a server can verify your
identity without ever holding anything sensitive. The cost is
operational pain — users have to manage certificate files, browsers
have to import them, proxies expire and have to be regenerated, and
revoking a stolen certificate is genuinely hard. DUNE used to live on
X.509 because that's what the grid offered; the field has now mostly
moved on to OIDC for user-facing flows.

---

## Slide 7 — A common confusion: cert vs key

**Visual:**

> **Private key**  = the unforgeable signature
> **Certificate** = the public-facing ID badge

Both come as a pair. The key proves the cert is yours.

Analogy:
- Cert ↔ a driver's licence (it's public — anyone can look at it)
- Key ↔ the unique skill of *being you* (you can sign things only because you are you)

**Narrative:**

People mix these up constantly. The certificate is the *public* part —
it's an ID badge anyone can look at, signed by a trusted authority so
they know it's real. The key is the *private* part — the thing that
lets you actually sign things, that nobody else has. They always come
as a pair, and the system only works because the key never leaves your
machine. The first time someone hands you a `.p12` or `.pem` file,
remember: the `.crt` half is the badge, the `.key` half is the magic
that proves the badge is yours.

---

## Slide 8 — Layer 3: OAuth 2.0 — delegation, not login

**Visual:**

```
You (resource owner)
  ↓ "this app may read my photos"
Authorization server  ──issues──▶  access token
                                       │
App  ──Authorization: Bearer <token>──▶ Photo API
```

- OAuth was designed for **delegation**, not login:
  "let app B do X on my behalf at service C"
- The output is an **access token** — proof of permission
- It deliberately does *not* tell C who you are

**Narrative:**

OAuth 2.0 is famously misunderstood because the original problem it was
designed to solve was not login at all. It was: "I want to let
Instagram print my Facebook photos, but I don't want to give Instagram
my Facebook password." The solution is an *access token* — a piece of
paper that says "the holder may read photos from Facebook for the next
hour." Notice what's missing — it doesn't say who you are. That gap is
what OIDC fills.

---

## Slide 9 — Layer 4: OIDC = OAuth 2.0 + identity

**Visual:**

```
OAuth gives you:   access_token   ("here is permission")
OIDC adds:         id_token       ("and here is who the user is")
```

- An OIDC `id_token` is a **JWT** with claims like
  `sub`, `email`, `iss`, `exp`
- Every "Sign in with Google" button you've ever clicked is OIDC
- **DUNE use:** CILogon → FNAL IAM → vault is an OIDC pipeline

**Narrative:**

OIDC — OpenID Connect — is the layer that takes OAuth and turns it
into actual login. The access token still gets issued; in addition,
you get an ID token whose only job is to tell you who the user is. The
ID token is signed by the issuer, so the receiver can verify it
without phoning home. This is the auth method behind essentially every
"Sign in with X" button you've ever clicked, and it's what dunecat,
metacat, and Rucio all use today.

---

## Slide 10 — Layer 5: Bearer tokens

**Visual:**

```
HTTP request:
  GET /datasets HTTP/1.1
  Authorization: Bearer eyJhbGciOi...
```

- "Bearer" means: **whoever holds this token gets in**
- No further proof of identity is needed
- Implications:
  - Must travel over HTTPS only
  - Must be short-lived
  - Must be kept secret
- It's a *header convention*, not a format

**Narrative:**

A bearer token works like a hotel keycard. The door doesn't check
whose name is on the card — it just checks that the card is valid. If
you drop it in the hall, the next person to pick it up can walk into
your room. That property makes bearer tokens easy to use but
relatively unforgiving if they leak, which is why they always travel
over HTTPS, always expire quickly, and always live in protected
storage. Critically, "bearer" is a *delivery* mechanism — it tells you
how the token is sent in an HTTP header. The token inside could be
anything.

---

## Slide 11 — Layer 6: JWT — a portable token format

**Visual:**

```
header.payload.signature
   │       │        │
   │       │        └─ signed by the issuer's private key
   │       └────────── base64-encoded JSON, e.g.
   │                   { "sub": "abc-123", "iss": "cilogon.org/dune",
   │                     "aud": "...", "exp": 1750000000 }
   └────────────────── { "alg": "RS256", "kid": "..." }
```

- **Self-describing:** any service with the issuer's public key can verify it offline
- **Readable:** it's just base64'd JSON — you can paste one into jwt.io
- **Not encrypted by default** — signed, but contents are visible

**Narrative:**

A JWT is the format that wraps up all the claims an OIDC issuer wants
you to know. Three base64 chunks separated by dots: the header (which
algorithm signs it), the payload (the actual claims — who the user is,
when the token expires, who issued it, who it's for), and the
signature. Two important properties: any service that knows the
issuer's public key can verify the JWT *without* contacting the
issuer, which makes JWTs cheap to scale; and the payload is *readable*
— it is not encrypted, only signed, so anyone with the token can see
what's in it. Paste any JWT into jwt.io and you'll see the JSON
immediately.

---

## Slide 12 — JWT vs opaque tokens

**Visual:**

|                       | JWT                                  | Opaque                                |
|-----------------------|--------------------------------------|---------------------------------------|
| What it is            | Signed, self-describing JSON         | Random string                         |
| Verification          | Anyone with the public key, offline  | Must be looked up at issuer           |
| Revocation            | Hard — valid until expiry            | Easy — delete from server's DB        |
| Contents              | Readable (base64)                    | Opaque to everyone but the issuer     |
| **In dunecat**        | OIDC bearer; metacat session         | Hub session cookie (`sessions` table) |

**Narrative:**

Tokens come in two broad flavours. A JWT is self-describing — the
receiver can verify it without any network calls. An opaque token is
just a random string; to know what it means, you have to look it up in
a database. The trade-off is reversed: JWTs scale beautifully because
verification is local, but they can't be revoked instantly — they're
valid until they expire. Opaque tokens can be revoked the moment you
delete the row, but every verification costs a database lookup. In
dunecat, the OIDC bearer is a JWT (verified by metacat and Rucio
without phoning home), while the hub's browser session cookie is
opaque — a random ID that the hub looks up in its own `sessions`
table.

---

## Slide 13 — Refresh tokens vs access tokens

**Visual:**

```
Refresh token  (long-lived, ~weeks)   ─┐
   ↓ "give me a fresh one"             │ exchange
Access token  (short-lived, ~hours)   ─┘
   ↓ used on every API call
Resource server
```

- **Access token:** short-lived. What gets sent to every API call.
  If stolen: limited damage (expires soon).
- **Refresh token:** long-lived. Held by a trusted client to mint new
  access tokens.
  If stolen: bigger damage — protected accordingly.

**Narrative:**

Why are there two tokens? Because of a tension between security and
convenience. You want the credential that's on the wire to be
short-lived, so if it leaks the blast radius is small. But you also
don't want to make the user log in via browser every hour. The OAuth
answer is a refresh token: a long-lived credential that lives in a
trusted place (a vault server, a secure config file, an encrypted
database row) whose only job is to mint short access tokens on demand.
In dunecat, the FNAL vault holds the refresh; htgettoken or the hub
backend turns it into a bearer JWT each time it's needed.

---

## Slide 14 — Device-code flow: when the client has no browser

**Visual:**

```
1. Client → IdP: "give me a code"
2. IdP    → Client: "show user code XYZ, tell them to visit https://..."
3. User opens URL on phone/laptop, enters XYZ, logs in
4. Client polls IdP: "are they done yet?"
5. IdP    → Client: "yes, here is the token"
```

- Used when the program needing the token can't itself host a browser:
  CLIs, servers, IoT devices
- **dunecat use:** `dunecat login` (local) and `/hub/login` (hub) both
  use device-code flow against `htvaultprod.fnal.gov`

**Narrative:**

OIDC's most common flow assumes the client *is* a browser and can
redirect freely. CLIs and headless servers can't do that. The
device-code flow is the workaround: the program shows you a short
code, you visit a URL on any browser, log in normally, and the program
polls the identity provider until you're done. That's exactly what
`dunecat login` and the hub's `/hub/login` page do — the user
authenticates in their normal browser session, and the backend polls
the vault server until the flow completes.

---

## Slide 15 — The Fermilab token machine

**Visual:**

```
You ──CILogon──▶ federated login (Google, your university, etc.)
                       │
                       ▼
                 FNAL IAM (maps you to a DUNE account)
                       │
                       ▼
                 htvaultprod.fnal.gov (holds refresh token)
                       │
                       ▼  on demand
                 Bearer JWT (~3 h, iss=cilogon.org/dune)
                       │
                       ▼
                 metacat, Rucio, dCache, ...
```

**Narrative:**

DUNE's authentication stack is several layers deep, but each layer has
a clear purpose. CILogon is the federated identity broker — it's how
you sign in with your home institution's credentials. FNAL IAM is the
mapping layer that ties your CILogon identity to a DUNE account.
Fermilab vault, reachable as `htvaultprod.fnal.gov`, is the refresh
token store — once you've authenticated through the device flow, vault
remembers you for the next 10 to 28 days. And finally vault mints
short-lived bearer JWTs on demand, which metacat, Rucio, and friends
all accept on their HTTP APIs.

---

## Slide 16 — dunecat local: how it stitches together

**Visual:**

```
dunecat login
  ├── htgettoken          → /tmp/bt_u<uid>     (bearer JWT, ~3 h)
  └── metacat auth login  → ~/.token_library   (metacat session, ~3 h)

Per-request: web server checks expiries and re-runs the above
             if a credential is within ~5 min of expiry.
Browser flow needed: every ~10 days (vault refresh rolls).
```

**Narrative:**

The local single-user dunecat shells out to two CLIs that have been
around for years. `htgettoken` drives the vault device flow and
deposits a bearer JWT at `/tmp/bt_u<uid>`. Then `metacat auth login`
takes that bearer and exchanges it for a metacat-issued session
token, which lands at `~/.token_library`. The dunecat web server
watches both files' expiries, and if either gets within five minutes
of expiry it shells out under a mutex to refresh — so you can leave
a tab open all day and not see auth errors, as long as your
underlying vault refresh hasn't rolled (which happens every 10 days).

---

## Slide 17 — dunecat hub: the multi-user variant

**Visual:**

```
Browser ──cookie──▶ hub backend
                       │
                       ▼
                  sessions table → users table → vault_tokens table
                                                    (AES-GCM encrypted)
                       │
            per-request:
                       ▼
                  vault → bearer JWT (~150 ms, in-memory only)
                       │
                       ▼
                  metacat / Rucio
```

- **No** `/tmp/bt_*`, **no** `~/.token_library`, **no** subprocesses
- Browser holds an opaque session cookie (7-day sliding TTL)
- DB holds the encrypted vault token (28-day lease)

**Narrative:**

The hub variant does the same auth dance but server-side, and per-user.
A user's browser holds only an opaque session cookie — meaningless on
its own. The cookie maps to a row in the `sessions` table, which maps
to a row in the `users` table, which has an encrypted vault token in
`vault_tokens`. On every API request, the hub decrypts that vault
token, mints a fresh bearer, and uses it to call metacat. Nothing
sensitive lives on disk in cleartext, nothing leaks between users, and
the user only sees the CILogon browser flow once every four weeks.

---

## Slide 18 — Why are there so many tokens?

**Visual:**

> Each layer's lifetime is bounded by the layer above it.

| Token              | Lifetime  | Why this length                                              |
|--------------------|-----------|--------------------------------------------------------------|
| Vault refresh      | 10–28 d   | Renewing it forces a browser session — expensive UX          |
| Bearer (access)    | ~3 h      | What metacat/Rucio actually see — small blast radius on leak |
| Metacat session    | ~3 h      | Inherits bearer's expiry (security invariant)                |
| Hub session cookie | 7 d (sliding) | Decoupled from FNAL — about user convenience              |

**Narrative:**

It looks like a lot of tokens, but each one has a job. The vault
refresh is long because forcing a browser dance is expensive. The
bearer is short because it's the credential that actually rides every
API call — if it leaks, you want the damage to be bounded. The
metacat session is derived from the bearer, so it's clamped to the
bearer's expiry — a derived credential can't outlive what
authenticated it. And the hub's own session cookie is about *your*
convenience: it has nothing to do with FNAL credentials, just whether
your browser is the same browser as five minutes ago.

---

## Slide 19 — Trade-off table: the schemes head-to-head

**Visual:**

|                      | Password               | X.509 cert + key         | OIDC bearer (JWT)         |
|----------------------|------------------------|--------------------------|---------------------------|
| Who carries secret   | Server (hash) + user   | User only                | User + IdP                |
| If stolen            | Catastrophic, all sites | Catastrophic, all servers trusting CA | Limited — short expiry, single audience |
| Revocation           | Server-side, easy      | CRL / OCSP — slow, painful | Wait for expiry (JWT) or revoke at IdP (opaque) |
| Expiry               | None unless enforced   | Months to years          | Hours (access) / weeks (refresh) |
| UX cost              | Low                    | High (cert management)   | Medium (browser flow at first login) |
| Standardised?        | Ad-hoc                 | RFC 5280                 | RFC 6749 + OIDC Core 1.0  |

**Narrative:**

If you put the three schemes side by side, the trajectory is clear.
Passwords are the simplest but worst — one leak compromises every
account that reuses them, and they don't expire on their own. X.509 is
cryptographically the strongest but operationally the most painful —
the user has to manage files and the revocation story is messy.
OIDC bearers split the difference: the secret on the wire is short-
lived so a leak has bounded impact, the user does a normal browser
flow they're used to, and revocation happens centrally at the
identity provider. That's why DUNE moved to OIDC, and that's why
dunecat speaks OIDC end-to-end.

---

## Slide 20 — Take-aways

**Visual:**

1. **Authentication is a flow**, not a token. The output of the flow is a credential.
2. **Bearer is a header, JWT is a format, OIDC is a protocol** — they sit on different axes.
3. **DUNE's stack is OIDC**: CILogon authenticates → FNAL IAM identifies → vault refreshes → bearer JWT authorises each call.
4. **dunecat exists in two flavours**: local (CLI shells out, files on disk) and hub (server-side, encrypted DB, per-user).
5. **When you see "401" in dunecat**, the error message tells you which layer broke and which command to run.

**Narrative:**

If you take three things away, take these. First, "authentication" is
a verb, not a noun — it names a process that produces a credential.
Second, the words bearer, JWT, and OIDC live on different layers and
combine freely; don't treat them as synonyms. Third, every confusing
file in your home directory — `/tmp/bt_u<uid>`, `~/.token_library`,
`~/.globus/`, `~/.dunecat/hub.key` — has a place in the picture you
now have. When something breaks, you can ask "which layer is this?"
instead of "what is this gibberish?"

---

## Slide 21 — Further reading

**Visual:**

- `docs/auth.md` — what `dunecat login` does, day-to-day operation
- `docs/hub.md` — multi-user hub: schema, login flow, encryption
- `docs/auth-faq.md` — short Q&A for general readers
- RFC 6749 (OAuth 2.0), OIDC Core 1.0 — the standards
- [jwt.io](https://jwt.io) — paste a JWT, see the JSON inside
- Fermilab Authentication Working Group docs (internal) for the IAM/vault story

**Narrative:**

If you want to dig further: the repo has two operational docs already
— `docs/auth.md` for the local app and `docs/hub.md` for the
multi-user variant — and a companion FAQ that complements these
slides. The RFCs are surprisingly readable if you want to see the
protocols in their original form. And jwt.io is the single best tool
for demystifying any JWT you encounter: paste it in, see exactly what
the issuer is telling the world about you.

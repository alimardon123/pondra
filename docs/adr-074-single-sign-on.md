# ADR-074: Single sign-on, through every door

**Date:** 2026-10-13 · **Status:** accepted; part 1 built (the owner chose "Sign-on first" on 10-13 at 10:27:
"The security lane builds single sign-on by the freeze; filters and masks come right after 1.0") ·
**Builds on:** ADR-035 (users, roles, grants; one check at every door), ADR-058 (a lake's own keys,
sealed), ADR-039 (a lake's format), ADR-049 (one registry of kinds), invariants 176, 177, 372, 376,
379, 380, 381 · **Thread:** Durability and upgrades (L7) · **Background:** `reviews/platform-comparison.md`
(governance row: Databricks, Snowflake and Fabric all sign in through the company's identity provider)

## Context

A company lets people try a database at work only when they sign in with the company's account, so
that leaving the company ends their access and IT sees who signed in. Today Pondra has its own users
(a password kept as SCRAM's verifier, tokens kept as hashes, sessions signed by the lake's key). An
admin makes each user and hands out each password. Nothing ties a user to Okta, Entra ID, Google,
Auth0 or Keycloak.

Every one of those speaks OpenID Connect: it signs a JSON Web Token (a JWT) naming the person, and
publishes the keys that check it (`/.well-known/openid-configuration`, then `jwks_uri`). The tools
people already use know how to fetch such a token and send it:

- HTTP clients and SDKs send it as `Authorization: Bearer <token>`.
- Postgres drivers send it as the password. Azure's Postgres signs in with Entra ID this way, and
  AWS's RDS with its IAM tokens. Postgres 18 added the SASL mechanism `OAUTHBEARER`, and `psql` 18
  can fetch the token itself (`oauth_issuer`, `oauth_client_id`: the device flow).
- Kafka clients use SASL/OAUTHBEARER (KIP-255), and fetch tokens from an OIDC provider themselves
  (KIP-768; librdkafka's `sasl.oauthbearer.method=oidc`).
- Arrow Flight and ADBC send a bearer token, or a name and password in their handshake.
- MCP clients find the provider through the resource's metadata (RFC 9728) and send a bearer token.
- A browser is sent to the provider and comes back with a code (the authorization code flow, with
  PKCE).

So single sign-on is mostly one check, done well, and a few doors taught to carry its token.

## Decision

### 1. An identity provider is an object

```sql
CREATE IDENTITY PROVIDER okta (
  ISSUER 'https://acme.okta.com',
  CLIENT_ID '0oa1b2c3',          -- the console's app at the provider; also an audience tokens may carry
  SECRET 'okta_client',          -- optional: a secret's name, holding the client's secret (a confidential client; Google needs one)
  AUDIENCE ('api://pondra'),     -- optional: other audiences (drivers', services')
  USER_CLAIM 'email',            -- optional: which claim names the user (email)
  ROLES_CLAIM 'groups',          -- optional: a claim whose values are roles (none)
  USERS 'listed',                -- optional: 'listed' (only users made here) or 'any'
  SCOPES 'openid email profile', -- optional: what the console asks for
  LABEL 'Okta'                   -- optional: the console's button
);
DROP IDENTITY PROVIDER okta;
COMMENT ON IDENTITY PROVIDER okta IS '…';
```

It is one `KINDS` entry (ADR-049): kind and family `identity provider`, prefix `ip/`, no privileges
(a superuser's DDL, as users are), `OnClone::Copy` (a branch's people sign in to it as to its base,
as its users are copied), and declared by no project (`project`: it is each environment's, as users
are). It takes the options' parenthesised form `CREATE SECRET` takes, and `CREATE OR REPLACE` to
change one (`ALTER` is refused, saying so). A client secret is never one of its options: `SECRET`
names a `CREATE SECRET` that holds it, sealed as every secret's values are (invariant 177), so the
provider itself holds nothing secret and has a `SHOW CREATE` that runs again after a drop.
`pondra.objects` lists it for an admin; `GET /sso` lists each provider's name, label and issuer
(that is public: the console's sign-in dialog draws a button from it).

Several providers may exist (one for people, one for services, or two companies'). A token is
matched to the providers whose issuer is its `iss` and whose audiences hold its `aud`, in name
order, and the first one whose checks pass signs it in.

### 2. How a token is checked (invariant 409)

Where the token arrives, by the node it arrives at, against the provider alone. Nothing is written
and the leader isn't asked.

- **Its shape:** a JWT of at most 16 KB, signed with RS256, RS384, RS512, PS256, ES256 or ES384
  (aws-lc-rs, already built: no new dependency). `none` and the HMAC algorithms are refused (a public
  key taken for an HMAC secret is the classic forgery).
- **Its key:** the `kid` in its header, from the provider's JWKS. The JWKS comes from the issuer's
  own discovery document, whose `issuer` must equal the provider's (OpenID Connect Discovery §4.3),
  fetched over HTTPS (HTTP only to this machine, for tests), 1 MB and 10 s at most. Keys are kept per
  node, fetched again when a token names a `kid` they lack (at most once a minute a provider, so
  random `kid`s can't make the node hammer the provider), and once they are an hour old, beside the
  sign-in that finds them so (which goes on with the key it has): a key the provider takes away stops
  signing in within the hour, and no sign-in waits on the provider for it.
- **Its claims:** `iss` exactly the provider's; `aud` one of `CLIENT_ID` and `AUDIENCE`; `exp` after
  now and `nbf` before it, with 60 s of leeway; the user claim present.
- **Opaque tokens** (Google's access tokens, some providers' by default) can't be checked without
  asking the provider each time. They are refused by name: "this provider's tokens aren't JWTs: send
  its ID token, or set it to issue JWT access tokens". Introspection can come after 1.0.

### 3. Who a token signs in as (invariant 410)

- **The user** is the user claim's value (`email` by default). A token whose `email_verified` is
  `false` is refused when the claim is `email`: an unverified address names nobody.
- **`USERS 'listed'`** (the default): only a user made here (`CREATE USER "ana@acme.com"`, with no
  password) signs in, with its own grants and roles. A role never signs in, as today.
- **`USERS 'any'`**: anyone the provider vouches for signs in, with `public`'s grants and its
  claimed roles, and no user is made for it. `CREATE TOKEN` and per-user grants still need the user
  made first. It is for the company's own provider: at one anyone may sign up to (Google's
  `accounts.google.com`, a public Auth0 tenant), every account there gets in, so use `'listed'` there.
- **`ROLES_CLAIM`**: each of the claim's values (a list, or words in a string) that names a role here
  makes the person a member of it for that sign-in. Values naming no role are ignored. So IT keeps
  who is in which group, and an admin grants to the roles. A claimed role that is a superuser's makes
  a superuser, as membership does today.
- **The principal** is built by `users::principal` with the claimed roles added, and cached by name
  and roles for the catalog version, as today. The history, the audit log, quotas and ownership see
  the user's name, as for any user.

### 4. Every door (invariant 410)

The check lives in two places every door already goes through:

- **`users::who`** (a bearer token): HTTP and the clients, Flight's `authorization`, MCP. A bearer
  token shaped like a JWT goes to the providers.
- **`users::sign_in`** (a name and a password): HTTP Basic, the Postgres port, Kafka's SASL/PLAIN,
  Flight's handshake. A password shaped like a JWT goes to the providers, and signs in only if it
  names that user. `users::is_token` counts it as a token (it ends within the hour), so under
  `PONDRA_TLS=optional` it may cross a plain connection as a token may (invariant 380).

Then a few lines at the doors that choose how to ask:

- **Postgres** asks for a cleartext password (in TLS from another machine) when the user has no
  password (only tokens, or none and a provider takes `'any'`): today's rule for a service's user.
  A user with a password still signs in with SCRAM. `psql` 18's `OAUTHBEARER` comes in §6's third
  part.
- **Kafka** takes the SASL mechanism `OAUTHBEARER` beside `PLAIN` (RFC 7628's
  `n,,\x01auth=Bearer <token>\x01\x01`), so `sasl.oauthbearer.method=oidc` clients work as they do
  with Confluent.
- **MCP** answers `GET /.well-known/oauth-protected-resource` (RFC 9728) naming the providers'
  issuers, and its 401 says where that is (`WWW-Authenticate: Bearer resource_metadata=…`).

A failed sign-in is refused, audited and counted as any failed sign-in is (invariant 381; a bearer
token isn't counted, as today).

### 5. The console's redirect (invariant 411)

The node finishes the flow itself (a backend for the page), so it works with every provider (Google
needs a client secret) and no provider token is ever kept in the browser:

1. The sign-in dialog shows a button for each provider (`GET /sso`). It opens
   `/sso/{provider}/start?to=<the page's own path>`.
2. The node makes a state, a nonce and a PKCE verifier, puts them with the provider and the path in a
   cookie signed with the lake's session key (`HttpOnly`, `Secure` over TLS, `SameSite=Lax`, ten
   minutes), and redirects to the provider's `authorization_endpoint`.
3. The provider sends the browser back to `/sso/callback` with a code and the state. The node checks
   the state against the cookie, exchanges the code at the `token_endpoint` (with the verifier, and
   the client secret if there is one), checks the ID token as §2 says, and its `nonce` against the
   cookie's.
4. It answers with a session (`ps_…`, `PONDRA_SESSION_HOURS`), exactly as `POST /login` does, handed
   to the page in the URL's fragment (which no server sees) on a redirect to the path it started from.
   The page keeps it as it keeps `/login`'s.

The redirect URI the provider is told is the node's own: `--advertise`'s address, else the request's
`Host` (checked as invariant 372 checks it), plus `/sso/callback`; behind `pondra serve --lakes`,
`/db/{name}/sso/callback`. Any node of the lake can finish a flow another started: the cookie is
signed with the lake's key, and nothing is kept on a node. `to` is only a path on this node (it starts
with one `/`), or for the command line a loopback address (§6).

### 6. In three parts, by the freeze

1. **The object, the check and every door** (by 10-18): §1 to §4, a stand-in provider
   (`tools/fake_idp.py`: discovery, JWKS, RS256 and ES256 tokens, a code flow), `harness.py sso`, the
   lake's format (§7). `harness.py sso` is the doors check for signing in (HTTP's Bearer and Basic,
   Postgres, Kafka's PLAIN and OAUTHBEARER, Flight, MCP): the doors matrix runs on an open node, which
   a provider would close.
2. **The console and the command line** (by 10-24): §5; `pondra login <url>` opens the browser to
   `/sso/{provider}/start?to=http://127.0.0.1:<port>/` (a loopback redirect, RFC 8252), keeps the
   session in the user's own file (`0600`), and the shell, `pondra sql`, the project commands and
   Python's and JavaScript's `connect(url)` use it when given no token.
3. **What finishes it** (by 10-29): Postgres's `OAUTHBEARER` for `psql` 18's device flow; a reference
   page and a guide (Okta, Entra ID, Google, Keycloak); a run against a real provider (Keycloak in a
   container in `deploy.yml`).

### 7. Never open, and only once every node knows it (invariant 412)

- A lake with an identity provider is never open: `users::any` counts providers, and making one
  writes `z/signed` in its commit (invariant 379), which 0.34's nodes honour too.
- A provider is made only at format 5, once every live node knows providers (ADR-039, as invariant
  377 waits for format 4). A node from before would take a token for nothing (refusing it) and, on a
  0.33 node of a lake with no other user, the lake for open.

## What it costs

- Nothing when unused (principle 6): no provider, no fetch, no check.
- A sign-in by token: one signature check (about 20 µs for RS256) and a cached principal; a key fetch
  an hour a provider a node at most.
- No new dependency: aws-lc-rs checks the signatures, reqwest fetches keys.
- About 700 lines: the kind and its statement, the check, the flow, the doors' few lines.

## What it doesn't do

- **Ending a connection when its token ends:** a Postgres, Kafka or Flight connection signed in with a
  token stays signed in after the token's `exp`, as it does with a password that changes meanwhile.
  Each new connection, and each HTTP request, is checked again.
- **SCIM** (the provider pushing users and groups): `USERS 'any'` with `ROLES_CLAIM` covers what SCIM is
  mostly used for. After 1.0, if asked for.
- **SAML**: every provider named above speaks OIDC. After 1.0, if asked for.
- **Ending a session when the provider disables someone:** a session lasts its hours. A token sent
  directly ends with the token (an hour, typically). Dropping the user or the role ends it at once.
- **Row filters and column masks:** right after 1.0 (the owner's choice), not here.

## Alternatives

- **Settings in the environment** (`PONDRA_OIDC_ISSUER`, as Trino and ClickHouse use a file): one
  provider per process, not a lake's, not the same on every node unless each is told, and invisible to
  SQL. An object is defined once, in the lake, and every node reads it (principle 9).
- **Users made at their first sign-in** (just-in-time): a write on the sign-in path, through the
  leader. `USERS 'any'` gives the same access with nothing written.
- **The browser exchanging the code itself** (a public client): no client secret on the node, but
  Google's web clients need one, the provider must allow the page's origin, and its tokens would sit
  in the browser. The node finishing the flow is what Databricks and Snowflake do.

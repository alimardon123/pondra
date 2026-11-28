//! Signing in (redesign step 7, piece 2): what every door does with a credential, in one place. A door reads its wire's
//! credential (`auth::Credential`: an `Authorization` header, Kafka's SASL PLAIN, Postgres's password message) and asks an
//! `Attempt`, which owns what each door did by copy before:
//! - the lock: past ten failures a minute for an address and a name, that pair is refused at once, the right password
//!   too (`auth::guessing`);
//! - whom it signs in (`users::who_at`; a token as the password signs in the token's principal, whatever the name);
//! - a refused password counted against that address and name (a token sent as the password is no guess at one, as it is
//!   never checked as one: not counted, invariant 381);
//! - 400 ms before every refusal, so a guess costs time at every door alike;
//! - the audit line, and the words: the same sentence at every door, each door adding only its own code (`Kind`); a
//!   token signed in under another name than its principal's is audited naming both (`audit::signed_as`).
//!
//! A door keeps only its own: its wire's codes, whether a password may cross a plain connection (`tls`), and what
//! a credential that signs nobody in is where one isn't needed (an open node's caller, the shell's own). SCRAM's two
//! messages are here too (`scram_first`, `scram_last`, piece 3); OAUTHBEARER's error round (piece 4) moves in next.
use crate::app::App;
use crate::auth::{Credential, Principal};

/// What a refusal costs, at every door.
pub const PAUSE: std::time::Duration = std::time::Duration::from_millis(400);

/// What a wrong name or password is told, at every door.
pub const WRONG_PASSWORD: &str = "wrong user name or password";

/// What a token that signs nobody in is told (an identity provider's says why instead: `sso::why`).
pub const WRONG_TOKEN: &str = "wrong token";

/// One sign-in at a door: from where, and what it is audited as.
pub struct Attempt<'a> {
    pub app: &'a App,
    pub door: &'static str,                 // (the audit's: "http", "postgres", "flight", "kafka")
    pub from: Option<std::net::SocketAddr>, // the connection's
    pub client: Option<std::net::IpAddr>,   // the one counted: behind a proxy, the client it names (`tls::Peer::client`)
    pub what: String,                       // (the audit's: "sign in", or HTTP's request)
    pub vouched: bool,                      // (from this machine, or with the nodes' authority's certificate: `users::nodes_key`)
}

/// Why a sign-in was refused, which each door says in its own codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// A name and a password that sign nobody in (Postgres's 28P01).
    Password,
    /// A token that signs nobody in, or an identity provider's token, which says why.
    Token,
    /// Its address and name past ten failures a minute: wait this many seconds (`auth::guessing`).
    Locked(u64),
    /// A credential that can't be read (`Credential::Malformed`).
    Malformed,
}

/// A refused sign-in: its kind and what the client is told.
#[derive(Debug, Clone)]
pub struct Refusal {
    pub kind: Kind,
    pub words: String,
}

/// What a credential came to.
pub enum Tried {
    Signed(Principal),
    /// Nobody: none sent, or one that signs no one in, and why when a token says (`users::who_or_why`). Where a sign-in
    /// is needed the door ends it with `Attempt::refuse`.
    Nobody(Option<String>),
    Refused(Refusal),
}

impl Attempt<'_> {
    /// An attempt counted by the connection's own address (the doors whose wire names no client behind a proxy).
    pub fn at<'a>(app: &'a App, door: &'static str, from: Option<std::net::SocketAddr>) -> Attempt<'a> {
        Attempt { app, door, from, client: from.map(|a| a.ip()), what: "sign in".into(), vouched: false }
    }

    /// Whom `credential` signs in: refused at once when it can't be read, or when its name is locked out from this
    /// address; `Nobody` when it signs no one in (not yet counted: `refuse`).
    pub async fn check(&self, credential: &Credential) -> Tried {
        if let Some(why) = credential.malformed() {
            return Tried::Refused(self.refused(Kind::Malformed, "", why).await);
        }
        if let Some(user) = credential.user() {
            if let Some(wait) = crate::auth::guessing(&self.app.lake, self.client, user) {
                return Tried::Refused(self.refused(Kind::Locked(wait), user, crate::auth::guessing_says(user, wait)).await);
            }
        }
        match crate::users::who_or_why(&self.app.lake, &self.app.auth, credential, self.vouched).await {
            Ok(p) => {
                // (a token under another name signs in its own principal: the audit names both)
                if let Some(given) = credential.user() {
                    crate::audit::signed_as(self.app, &p.name, given, self.door, self.from, self.client);
                }
                Tried::Signed(p)
            }
            Err(why) => Tried::Nobody(why),
        }
    }

    /// `check`, where a sign-in is needed: a credential that signs nobody in is refused (`refuse`).
    pub async fn sign_in(&self, credential: &Credential) -> Result<Principal, Refusal> {
        match self.check(credential).await {
            Tried::Signed(p) => Ok(p),
            Tried::Refused(r) => Err(r),
            Tried::Nobody(why) => Err(self.refuse(credential, why).await),
        }
    }

    /// A user's own password (`POST /login`'s: no token, which a session isn't made from), checked as `check` checks one.
    pub async fn password(&self, user: &str, password: &str) -> Result<(), Refusal> {
        let credential = Credential::Password { user: user.into(), password: password.into() };
        if let Some(wait) = crate::auth::guessing(&self.app.lake, self.client, user) {
            return Err(self.refused(Kind::Locked(wait), user, crate::auth::guessing_says(user, wait)).await);
        }
        match crate::users::password_ok(&self.app.lake, user, password).await {
            true => Ok(()),
            false => Err(self.refuse(&credential, None).await),
        }
    }

    /// SCRAM's first message (Postgres): refused at once when its name is locked out from this address, as `check` is;
    /// else the exchange begun and SCRAM's answer, under the name's verifier: a built-in name's token, a user's password,
    /// or, for a name with neither, one no password fits, so it is told what a wrong password is, at the end, and no sooner.
    pub async fn scram_first(&self, user: &str, first: &str) -> Result<(crate::users::Scram, String), Refusal> {
        if let Some(wait) = crate::auth::guessing(&self.app.lake, self.client, user) {
            return Err(self.refused(Kind::Locked(wait), user, crate::auth::guessing_says(user, wait)).await);
        }
        let verifier = match crate::users::BUILT_IN.contains(&user) {
            true => self.app.auth.token_for(user).filter(|t| !t.is_empty()).map(|t| crate::users::Verifier::of(&t)),
            false => crate::users::verifier(&self.app.lake, user).await,
        };
        let verifier = verifier.unwrap_or_else(|| crate::users::Verifier::of(&uuid::Uuid::new_v4().to_string()));
        match crate::users::Scram::first(verifier, first) {
            Ok(begun) => Ok(begun),
            Err(_) => Err(self.refused(Kind::Password, user, WRONG_PASSWORD.into()).await), // (a message that can't be read)
        }
    }

    /// SCRAM's last message: whom it signs in, and SCRAM's final answer; a wrong proof is a wrong password, counted as
    /// `refuse` counts one, except a built-in name's, whose password is its token, random and never guessed.
    pub async fn scram_last(&self, begun: crate::users::Scram, user: &str, last: &str) -> Result<(Principal, String), Refusal> {
        let built_in = crate::users::BUILT_IN.contains(&user);
        let Ok(reply) = begun.last(last) else {
            if !built_in {
                crate::auth::guessed(&self.app.lake, self.client, user);
            }
            return Err(self.refused(Kind::Password, user, WRONG_PASSWORD.into()).await);
        };
        let who = match built_in {
            true => Some(Principal::token(self.app.auth.role_of_user(user))),
            false => crate::users::principal(&self.app.lake, user).await.ok(),
        };
        match who {
            Some(p) => Ok((p, reply)),
            None => Err(self.refused(Kind::Password, user, WRONG_PASSWORD.into()).await),
        }
    }

    /// A credential that signed nobody in, refused: counted against its name from this address when it was checked as the
    /// name's password: any password, and a token's form too for a user who has one (`users::sign_in` tries it as theirs,
    /// 11-22 audit L1). A token in a token-only user's place guesses at nothing, so it isn't counted (invariant 381). Then
    /// audited, and said: `why` when a token said it (`Tried::Nobody`).
    pub async fn refuse(&self, credential: &Credential, why: Option<String>) -> Refusal {
        if let Credential::Password { user, password } = credential {
            let checked = !crate::users::token_form(&self.app.auth, password)
                || (!crate::users::BUILT_IN.contains(&user.as_str()) && crate::users::verifier(&self.app.lake, user).await.is_some());
            if checked {
                crate::auth::guessed(&self.app.lake, self.client, user);
            }
        }
        let (kind, words) = match (why, credential) {
            (Some(why), _) => (Kind::Token, why),
            (None, Credential::Password { .. }) => (Kind::Password, WRONG_PASSWORD.into()),
            (None, _) => (Kind::Token, WRONG_TOKEN.into()),
        };
        self.refused(kind, credential.user().unwrap_or_default(), words).await
    }

    /// Audited, then said after `PAUSE`, as every refusal is.
    async fn refused(&self, kind: Kind, user: &str, words: String) -> Refusal {
        crate::audit::refused_by(self.app, user, self.door, self.from, self.client, &self.what, &words).await;
        tokio::time::sleep(PAUSE).await;
        Refusal { kind, words }
    }
}

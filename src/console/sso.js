// A sign-in through an identity provider (ADR-074 §5), the page's side. `start` keeps a nonce in this tab and sends it
// in the address the sign-in ends at; `back` takes the session, or why it failed, from the fragment (which no server
// sees) only when the address carries that nonce, so a link can't sign the page in as someone else, nor put words in
// the sign-in dialog. The fragment and the nonce leave the address either way. Loaded by settings.js's buttons, and by
// core.js only when an address carries a sign-in's return; it imports nothing, so core.js can wait for it.
const KEPT = 'pondra.sso', PARAM = 'pondra_sso';

// To `url` (a provider's /sso/…/start), to come back to `at` (a path of this page's, with its query) with the nonce.
export function start(url, at) {
  const nonce = [...crypto.getRandomValues(new Uint8Array(16))].map(b => b.toString(16).padStart(2, '0')).join('');
  sessionStorage.setItem(KEPT, nonce);
  location.href = `${url}?to=${encodeURIComponent(at + (at.includes('?') ? '&' : '?') + PARAM + '=' + nonce)}`;
}

// The session into `store` (the page's token, as one typed in), and why a sign-in failed, or null.
export function back(store) {
  const got = new URLSearchParams(location.hash.slice(1)), asked = new URLSearchParams(location.search);
  let ours = false;
  try {
    ours = !!asked.get(PARAM) && asked.get(PARAM) === sessionStorage.getItem(KEPT);
    sessionStorage.removeItem(KEPT);
  } catch { /* (no sessionStorage: nothing was kept, so nothing is taken) */ }
  if (ours && got.get('pondra_session')) {
    store.set('pondra.token', got.get('pondra_session'));
    store.set('pondra.user', got.get('pondra_user'));
  }
  asked.delete(PARAM);
  history.replaceState(null, '', location.pathname + (asked.size ? '?' + asked : ''));
  return ours ? got.get('pondra_error') : null;
}

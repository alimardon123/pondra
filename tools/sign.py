#!/usr/bin/env python3
"""Signs a release binary the way its platform checks one, when the certificate is given, and
otherwise says so and leaves it as it is (a fork's pull request, a repository without them). It
goes between the build and tools/package.py, so every package carries the signed binary.

  sign.py target/aarch64-apple-darwin/dist/pondra          # macOS: codesign, then notarized
  sign.py target/x86_64-pc-windows-msvc/dist/pondra.exe    # Windows: signtool (Authenticode)

macOS: APPLE_CERTIFICATE (a Developer ID Application certificate with its key, .p12, base64) and
APPLE_CERTIFICATE_PASSWORD sign it (hardened runtime, Apple's timestamp); APPLE_API_KEY (an App
Store Connect API key, .p8, base64), APPLE_API_KEY_ID and APPLE_API_ISSUER have Apple notarize it
too (a bare binary can't be stapled: Gatekeeper asks Apple the first time it runs one). Windows:
WINDOWS_CERTIFICATE (.pfx, base64) and WINDOWS_CERTIFICATE_PASSWORD, timestamped by DigiCert.
Linux has no such signature: release.yml attests every archive (`gh attestation verify`).

Each signature is verified before this ends; any failure exits 1, so a release is never half
signed. deploy.yml tries it with a certificate made for the run.
"""
import base64, json, os, re, secrets, subprocess, sys, tempfile
from glob import glob

TIMESTAMP = "http://timestamp.digicert.com"


def run(*args, quiet=False):
    print("+", os.path.basename(args[0]), args[1], flush=True)  # (never the rest: passwords go there)
    r = subprocess.run(args, capture_output=True, text=True)
    if r.returncode:
        sys.exit(f"{args[0]} failed ({r.returncode}):\n{r.stdout}{r.stderr}")
    if not quiet and (r.stdout or r.stderr).strip():
        print((r.stdout + r.stderr).strip())
    return r.stdout


def secret_file(folder, name, var):
    path = os.path.join(folder, name)
    with open(path, "wb") as f:
        f.write(base64.b64decode(os.environ[var]))
    return path


def macos(binary, tmp):
    # A keychain of the run's own, unlocked, that codesign may use without asking: the runner's
    # login keychain is never touched, and the certificate goes with the keychain at the end.
    keychain, password = os.path.join(tmp, "sign.keychain-db"), secrets.token_hex(16)
    searched = re.findall(r'"([^"]+)"', run("security", "list-keychains", "-d", "user", quiet=True))
    run("security", "create-keychain", "-p", password, keychain)
    try:
        run("security", "set-keychain-settings", "-lut", "3600", keychain)
        run("security", "unlock-keychain", "-p", password, keychain)
        run("security", "list-keychains", "-d", "user", "-s", keychain, *searched)
        p12 = secret_file(tmp, "certificate.p12", "APPLE_CERTIFICATE")
        run("security", "import", p12, "-k", keychain, "-P", os.environ.get("APPLE_CERTIFICATE_PASSWORD", ""), "-T", "/usr/bin/codesign", quiet=True)
        run("security", "set-key-partition-list", "-S", "apple-tool:,apple:,codesign:", "-s", "-k", password, keychain, quiet=True)
        found = re.findall(r"\b([0-9A-F]{40})\b", run("security", "find-identity", "-p", "codesigning", keychain))
        if not found:
            sys.exit("APPLE_CERTIFICATE holds no identity that can sign code")
        run("codesign", "--force", "--options", "runtime", "--timestamp", "--keychain", keychain, "--sign", found[0], binary)
        run("codesign", "--verify", "--strict", "--verbose=2", binary)
        run("codesign", "--display", "--verbose=2", binary)
    finally:
        run("security", "list-keychains", "-d", "user", "-s", *searched)
        run("security", "delete-keychain", keychain)
    if os.environ.get("APPLE_API_KEY"):
        notarize(binary, tmp)


def notarize(binary, tmp):
    archive = os.path.join(tmp, "pondra.zip")  # (notarytool takes a zip, a disk image or a package)
    run("ditto", "-c", "-k", "--keepParent", binary, archive)
    key = secret_file(tmp, "key.p8", "APPLE_API_KEY")
    auth = ["--key", key, "--key-id", os.environ["APPLE_API_KEY_ID"], "--issuer", os.environ["APPLE_API_ISSUER"]]
    done = json.loads(run("xcrun", "notarytool", "submit", archive, *auth, "--wait", "--timeout", "30m", "--output-format", "json", quiet=True))
    print("notarization:", done.get("status"), done.get("id"))
    if done.get("status") != "Accepted":
        print(run("xcrun", "notarytool", "log", done.get("id", ""), *auth, quiet=True))
        sys.exit("Apple didn't accept the binary")


def windows(binary, tmp):
    tools = sorted(glob(r"C:\Program Files (x86)\Windows Kits\10\bin\*\x64\signtool.exe"), key=lambda p: [int(n) for n in re.findall(r"\d+", p)])
    if not tools:
        sys.exit("no signtool: install the Windows SDK")
    pfx = secret_file(tmp, "certificate.pfx", "WINDOWS_CERTIFICATE")
    run(tools[-1], "sign", "/f", pfx, "/p", os.environ.get("WINDOWS_CERTIFICATE_PASSWORD", ""), "/fd", "SHA256", "/tr", TIMESTAMP, "/td", "SHA256", "/d", "Pondra", binary)
    run(tools[-1], "verify", "/pa", "/v", binary, quiet=True)  # (the chain to a trusted root, and the timestamp)
    print("signed and verified:", binary)


def main():
    if len(sys.argv) != 2 or not os.path.isfile(sys.argv[1]):
        sys.exit(__doc__)
    binary = sys.argv[1]
    sign, need = {"darwin": (macos, "APPLE_CERTIFICATE"), "win32": (windows, "WINDOWS_CERTIFICATE")}.get(sys.platform, (None, None))
    if not sign:
        return print(f"{binary}: not signed ({sys.platform} binaries have no signature; release.yml attests the archives)")
    if not os.environ.get(need):
        return print(f"{binary}: not signed ({need} isn't set)")
    with tempfile.TemporaryDirectory() as tmp:
        sign(binary, tmp)


if __name__ == "__main__":
    main()

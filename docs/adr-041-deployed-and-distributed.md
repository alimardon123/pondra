# ADR-041: Deployed and distributed — an image, a chart, a service, the package managers

**Date:** 2026-10-02 · **Status:** accepted (built on `claude/deploy-and-packaging-im9gd7`;
publishing waits for the owner's accounts, §9) · **Builds on:** ADR-003 (serverless: nothing
always on but the nodes), ADR-005 (a node is its advertised address), ADR-018 (the glibc 2.17
binary, the wheels and npm packages from one binary), ADR-024 (the installers), ADR-027 (Python
beside the node), ADR-035 (TLS, tokens, the master key) · **Leaves to ADR-039:** drain before stop
and upgrades

## Context

The roadmap puts "a container image, a compose cluster, a Helm chart; `pondra service install`"
in round 33 and "signed binaries for Windows and macOS; Homebrew, winget and a container image" in
round 37. Until now Pondra ran as a binary a person starts, which is how it should start (principle
1), but not how a team keeps one running: on Kubernetes, under a machine's service manager, or
installed and upgraded by the tools people already use. Round 34 also needs machines stood up for
SF100 with as little by hand as possible.

The constraints are the product's own: no service of ours beside the nodes (principle 2, so no
operator and no daemon), the same binary everywhere (principle 1), and nothing that slows a
cluster or holds memory when unused (principle 6).

## Decision

1. **Nothing is built twice.** The image, the chart's pods, the services and the package managers
   all carry the binary the build workflow made and tested for that commit. `deploy.yml` compiles
   nothing: `tools/ci_artifact.sh` takes a platform's packages from the commit's build run
   (waiting while it runs, or reusing a pull request's run of the same tree, as `release.yml`
   does), and `tools/image.py` lays them out for `docker build`. A release's image, archives,
   wheels and npm packages hold the same bytes.

2. **The image** (`deploy/docker/Dockerfile`): distroless `cc` (glibc and CA certificates; no
   shell), user 65532, one volume `/data` holding the lake (`/data/lake`), the node's cache and its
   master key (`HOME=/data`, so `/data/.pondra/secret.key` lives as long as the volume); a
   read-only root works. amd64 and arm64 come from the two Linux binaries, without emulation. A
   `-python` variant (`python:3.13-slim` with the wheel and pyarrow) runs `--python auto`, since
   functions and procedures need a Python beside the node. Tags `X.Y.Z`, `X.Y`, `latest`, each
   with provenance, an SBOM and a keyless cosign signature (GitHub's identity: no key to keep).

3. **A node sizes itself to its container.** `store::ram()` takes the smaller of the machine's
   memory and its cgroup's limit (v2 `memory.max`, v1 `memory.limit_in_bytes`, the smallest over
   the cgroup's ancestors). Before, a node in a 1 GB container planned a third of the *machine's*
   memory for queries: more than the container may use, so the kernel kills it before it spills. `deploy_check.py image`:
   "its queries are sized by the container's memory, not the host's".

4. **Compose** (`deploy/compose/compose.yaml`): three nodes on one shared lake volume, each with
   its own cache, ports 8080–8082 and the Postgres, Flight and Kafka ports beside them; a bucket
   through `PONDRA_LAKE`. It is for trying a cluster on one machine.

5. **The Helm chart** (`deploy/helm/pondra`):
   - The writers are a StatefulSet: a pod's name is its address, which is who it is to the
     cluster (ADR-005), so a restarted pod resumes as itself. They start in parallel: no node
     waits for another, the first to claim the lake leads.
   - A headless service publishes addresses before readiness, so nodes find their leader while
     starting; readiness, liveness and startup are `/stats` (no token needed).
   - More than one node needs a lake every pod reaches: a bucket (`lake: s3://…`, credentials from
     `bucket.existingSecret`) or a ReadWriteMany claim. Anything else is refused at install with
     the reason (`fail`), not discovered at runtime.
   - Tokens are made once and kept across upgrades (`lookup`); the master key's Secret is kept even
     on uninstall (`helm.sh/resource-policy: keep`), because the secrets sealed in the lake can't
     be read without it.
   - Readers (`readers.replicas`, optional autoscaling) are a Deployment behind a `-read` service;
     the main service sends only to writers, since a reader refuses writes.
   - TLS from an existing secret (mutual if asked), a PodMonitor with the read token, a
     PodDisruptionBudget, 60 s to stop so a leader hands its term on.
   - No operator: what an operator would do (scale, restart, upgrade) is Kubernetes' own, and the
     cluster's state is the lake's.

6. **`pondra service install | status | uninstall`** (`src/service.rs`) hands the node to the
   machine's own manager, never a daemon of ours: a systemd unit (system-wide under sudo, running
   as the user who called sudo; a user unit otherwise, with lingering turned on), a launchd daemon
   or agent, a Windows service. The `serve` options are checked when installing, with `serve`'s own
   parser, and kept with the `PONDRA_*`, `AWS_*`, `AZURE_*` and `GOOGLE_*` variables in one file
   only its user reads. On Windows the binary is its own service host (`service run`): it runs
   `serve --stop-with-stdin` as a child and stops it by closing its input, so a leader gives up its
   term (invariant 46); a node that must restart to rejoin exits 75 there and is restarted, instead
   of spawning a process the manager doesn't know.

7. **Homebrew, Scoop and winget** (`tools/distribute.py`): a formula for the tap
   `alimardon123/homebrew-pondra` (`brew install alimardon123/pondra/pondra`, `brew services`), a
   manifest for the bucket `alimardon123/scoop-pondra`, and winget's three manifests for
   `Pondra.Pondra` (a zip holding one portable `pondra.exe`), all from the release's archives by
   SHA-256. `deploy.yml` installs this commit's archive with Homebrew on macOS and Linux and with
   Scoop on Windows on every pull request; on a tag it commits the formula and the manifest and
   opens winget's pull request.

8. **Signing** (`tools/sign.py`, between the build and `tools/package.py`, so every package holds
   the signed binary): macOS with a Developer ID certificate (hardened runtime, Apple's timestamp),
   then notarized (a bare binary can't be stapled: Gatekeeper asks Apple the first time); Windows
   with Authenticode, timestamped. Without certificates it does nothing and says so. Linux binaries
   have no such signature: every release archive gets GitHub's build provenance attestation
   (`gh attestation verify pondra-linux-x64.tar.gz -R alimardon123/pondra`). `deploy.yml` signs
   with a certificate made for the run, verifies it and runs the signed binary, every pull request.

9. **Publishing waits for the owner.** Nothing goes to a new registry until the repository says so:
   the variable `PUBLISH_IMAGE` for ghcr.io (no token: the workflow's own), `PUBLISH_PACKAGES` with
   the secrets `PACKAGES_TOKEN` (contents on the tap and the bucket repositories) and
   `WINGET_TOKEN` (a classic token with `public_repo`, for winget-pkgs' pull request), and the
   certificates' secrets for signing (`APPLE_CERTIFICATE`, `APPLE_CERTIFICATE_PASSWORD`,
   `APPLE_API_KEY`, `APPLE_API_KEY_ID`, `APPLE_API_ISSUER`; `WINDOWS_CERTIFICATE`,
   `WINDOWS_CERTIFICATE_PASSWORD`) with one step in `build.yml` (below).

10. **Machines for round 34** (`tools/cloud/`): scripts that stand three to six VMs up in one
    region, load SF100 into a bucket and run the bench across them, and take everything down, so a
    trial's credit pays only for the hours used.

## The step `build.yml` gets with the certificates

After **Build**, before `tools/package.py`:

```yaml
      - name: Signed (macOS, Windows), when the certificates are set
        env:
          APPLE_CERTIFICATE: ${{ secrets.APPLE_CERTIFICATE }}
          APPLE_CERTIFICATE_PASSWORD: ${{ secrets.APPLE_CERTIFICATE_PASSWORD }}
          APPLE_API_KEY: ${{ secrets.APPLE_API_KEY }}
          APPLE_API_KEY_ID: ${{ secrets.APPLE_API_KEY_ID }}
          APPLE_API_ISSUER: ${{ secrets.APPLE_API_ISSUER }}
          WINDOWS_CERTIFICATE: ${{ secrets.WINDOWS_CERTIFICATE }}
          WINDOWS_CERTIFICATE_PASSWORD: ${{ secrets.WINDOWS_CERTIFICATE_PASSWORD }}
        run: python tools/sign.py "$BIN"
```

## Consequences

- A team runs Pondra the way it runs other services, with nothing new to operate.
- Every way is tried on every pull request, on the packages the suite tested: `tools/deploy_check.py`
  (`image`, `python`, `compose`, `chart`, `helm` on kind, `service` on Linux, macOS and Windows)
  and the package managers' and signing steps in `deploy.yml`.
- A stopped leader hands over within the followers' lease (about 5 s on a bucket): until ADR-039's
  drain, a rolling restart of the chart pauses writes that long per leader.
- Not built: an operator, Terraform modules, a Debian or RPM repository, Chocolatey. Each can come
  when users ask; the archives and `pondra service` cover the same machines meanwhile.

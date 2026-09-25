#!/usr/bin/env node
// `npx pondra …`: the binary for this platform, with the arguments given.
import { spawnSync } from "node:child_process";
import { binary } from "../index.js";

const r = spawnSync(binary(), process.argv.slice(2), { stdio: "inherit" });
process.exit(r.status ?? 1);

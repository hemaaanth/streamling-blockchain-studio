# Pons Family demo

This directory is the Pons Family V2 dashboard for Robinhood Chain.

- Streamling is the only source of dashboard data. Do not hand-create event fixtures.
- Initialize the managed project with `python3 demos/pons-family/scripts/init_project.py` from the repository root. It reads `contracts.json` and uses only the generic `init` and `add-discovery` commands.
- The generated SQLite database lives at `.local/demos/pons-family/.streamling-blockchain/events.db` and must not be committed.
- Refresh Evidence with `STREAMLING_BLOCKCHAIN_DB=<generated-db> npm run sources:sqlite`.
- `sources/sqlite/events.sql` must preserve the shared event schema used by the Evidence pages.
- Pons V2 factory and bonding-curve event ABIs live in `abis/` and come from the official Pons contracts. Keep the factory address and deployment block aligned with the verified Robinhood Chain deployment.
- Pages must distinguish native-quote volume from ERC-20 quote volume; never sum different quote assets as one monetary value.
- Generated `.evidence/`, `build/`, `node_modules/`, linked databases, progress files, and production credentials are ignored and must not be committed.
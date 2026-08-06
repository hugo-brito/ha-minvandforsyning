# Runbook: removing sensitive data from Git history

Use this when a real meter number, a real API response, or other sensitive data has been committed. Rewriting history on a published HACS repository is disruptive and cannot recall existing clones, forks, or external caches. Treat it as a deliberate, gated decision, not a routine cleanup.

## Before you start

- Confirm the value is genuinely sensitive and worth a rewrite.
- Freeze merges and coordinate with collaborators and known fork owners.
- Never paste the sensitive value into chat, commands, commit messages, or filenames. Keep match patterns in a private file outside the repository.

## Procedure

1. Inventory every occurrence: reachable branches and tags, commit messages, historical blobs, pull request text and reviews, release assets, and Actions logs. Record affected commit and blob IDs without recording the plaintext.
2. Prepare a clean replacement first (synthetic fixtures, redacted text) so the rewritten default branch stays test-green.
3. Work in a disposable mirror clone, never in a normal working checkout. Keep an encrypted rollback bundle outside synchronized folders.
4. Use `git filter-repo` to remove the offending paths from all history and to replace the identifier in text blobs. Scrub affected commit messages separately: `--replace-text` does not touch commit messages or binary blobs.
5. Disable the release workflow so the cutover does not publish a release.
6. Force-update ordinary branches and tags. Never push `refs/pull/*`.
7. Close and recreate any affected pull request from the rewritten base. Do not reuse its old commit graph.
8. Ask GitHub Support to purge cached PR diffs, `refs/pull/*`, stale objects, and search indexes.
9. Recreate affected releases from preserved metadata and delete workflow artifacts that contain the value.
10. Verify from a fresh clone that the value and the old blobs are gone.

## Irreducible limits

Existing clones cannot be recalled. External caches and forks may retain the data until their owners act. Document what remains exposed.

## References

- GitHub docs: removing sensitive data from a repository.
- `git filter-repo`.
